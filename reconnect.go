package pgxreconnect

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/cenkalti/backoff/v4"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

var (
	ErrNoConnString = errors.New("no connection string provided")
	ErrReconnect    = errors.New("error while reconnecting")
)

type ReConn struct {
	ConnString string
	Conn       *pgx.Conn
	Backoff    backoff.BackOff
}

func Connect(ctx context.Context, connString string, bo backoff.BackOff) (*ReConn, error) {
	if connString == "" {
		return nil, ErrNoConnString
	}
	c := &ReConn{
		ConnString: connString,
		Backoff:    bo,
	}
	return c, c.Reconnect(ctx)
}

func (c *ReConn) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if err := c.checkConn(ctx); err != nil {
		return nil, err
	}
	rows, err := c.Conn.Query(ctx, sql, args...)
	if err != nil && reconnectable(err) {
		if reconnectErr := c.Reconnect(ctx); reconnectErr != nil {
			return nil, fmt.Errorf("%w: %w, original error: %w", ErrReconnect, reconnectErr, err)
		}
		return c.Conn.Query(ctx, sql, args...)
	}
	return rows, err
}

func (c *ReConn) QueryRow(ctx context.Context, sql string, args ...any) (pgx.Row, error) {
	if err := c.checkConn(ctx); err != nil {
		return nil, err
	}
	return c.Conn.QueryRow(ctx, sql, args...), nil
}

func (c *ReConn) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	if err := c.checkConn(ctx); err != nil {
		return pgconn.CommandTag{}, err
	}
	tag, err := c.Conn.Exec(ctx, sql, args...)
	if err != nil && reconnectable(err) {
		if reconnectErr := c.Reconnect(ctx); reconnectErr != nil {
			return pgconn.CommandTag{}, fmt.Errorf("%w: %w, original error: %w", ErrReconnect, reconnectErr, err)
		}
		return c.Conn.Exec(ctx, sql, args...)
	}
	return tag, err
}

func (c *ReConn) Reconnect(ctx context.Context) error {
	if c.Conn != nil {
		if err := c.Conn.Close(ctx); err != nil {
			// ignore
		}
		c.Conn = nil
	}
	if c.ConnString == "" {
		return ErrNoConnString
	}
	if c.Backoff == nil {
		c.Backoff = backoff.NewExponentialBackOff()
	}
	err := backoff.Retry(func() error {
		conn, err := pgx.Connect(ctx, c.ConnString)
		if err != nil {
			return err
		}
		c.Conn = conn
		return nil
	}, c.Backoff)
	if err == nil {
		return err
	}
	pe := new(backoff.PermanentError)
	if errors.As(err, &pe) {
		return pe.Err
	}
	return nil
}

func (c *ReConn) Ping(ctx context.Context) error {
	if err := c.checkConn(ctx); err != nil {
		return err
	}
	err := c.Conn.Ping(ctx)
	if err != nil && reconnectable(err) {
		if reconnectErr := c.Reconnect(ctx); reconnectErr != nil {
			return fmt.Errorf("%w: %w, original error: %w", ErrReconnect, reconnectErr, err)
		}
		return c.Conn.Ping(ctx)
	}
	return err
}

func (c *ReConn) Close(ctx context.Context) error {
	if c.Conn == nil || c.Conn.IsClosed() {
		return nil
	}
	err := c.Conn.Close(ctx)
	c.Conn = nil
	return err
}

func (c *ReConn) checkConn(ctx context.Context) error {
	if c.Conn != nil && !c.Conn.IsClosed() {
		return nil
	}
	return c.Reconnect(ctx)
}

func reconnectable(err error) bool {
	if strings.Contains(err.Error(), "conn closed") {
		return true
	}
	if pgconn.Timeout(err) {
		return true
	}
	if errors.Is(err, &pgconn.ConnectError{}) {
		return true
	}
	if pgconn.SafeToRetry(err) {
		return true
	}
	return false
}
