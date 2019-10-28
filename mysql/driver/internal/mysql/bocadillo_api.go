package mysql

import (
	"context"
	"database/sql/driver"
	"errors"
	"time"
)

// ExtendConn creates an extended connection.
func ExtendConn(conn driver.Conn) (*ExtendedConn, error) {
	if conn == nil {
		return nil, errors.New("Connection is nil")
	}
	mc, ok := conn.(*mysqlConn)
	if !ok || mc == nil {
		return nil, errors.New("Invalid connection")
	}

	return &ExtendedConn{mc}, nil
}

// ExtendedConn provides access to internal packet functions.
type ExtendedConn struct {
	*mysqlConn
}

// Close ...
func (c *ExtendedConn) Close() error {
	c.buf.length = 0
	return c.mysqlConn.Close()
}

// Exec ...
func (c *ExtendedConn) Exec(query string) error {
	return c.exec(query)
}

// ReadPacket reads a packet from a given connection.
func (c *ExtendedConn) ReadPacket(ctx context.Context) ([]byte, error) {
	if dl, ok := ctx.Deadline(); ok {
		dur := dl.Sub(time.Now())
		if dur < 0 {
			return nil, context.DeadlineExceeded
		}
		c.buf.timeout = dur
	} else {
		c.buf.timeout = 0
	}

	return c.readPacket()
}

// WritePacket writes a packet to a given connection.
func (c *ExtendedConn) WritePacket(p []byte) error {
	return c.writePacket(p)
}

// ReadResultOK ...
func (c *ExtendedConn) ReadResultOK() error {
	return c.readResultOK()
}

// HandleErrorPacket ...
func (c *ExtendedConn) HandleErrorPacket(data []byte) error {
	return c.handleErrorPacket(data)
}

// ResetSequence ...
func (c *ExtendedConn) ResetSequence() {
	c.sequence = 0
}
