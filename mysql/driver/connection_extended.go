package driver

import (
	"context"
	"time"
)

// ExtendedConn provides access to internal packet functions.
type ExtendedConn struct {
	*mysqlConn
}

// NewExtendedConnection creates a new connection extended with packet access
// methods.
func NewExtendedConnection(dsn string) (*ExtendedConn, error) {
	conn, err := newConnection(dsn)
	if err != nil {
		return nil, err
	}
	return &ExtendedConn{conn}, nil
}

// Exec executes a query.
func (c *ExtendedConn) Exec(query string) error {
	return c.exec(query)
}

// ReadPacket reads a packet from a given connection. If given context has a
// deadline it would be used as read timeout.
func (c *ExtendedConn) ReadPacket(ctx context.Context) ([]byte, error) {
	if dl, ok := ctx.Deadline(); ok {
		dur := time.Until(dl)
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

// ReadResultOK returns an error if packet is not an OK_Packet.
// Spec: https://dev.mysql.com/doc/internals/en/packet-OK_Packet.html
func (c *ExtendedConn) ReadResultOK() error {
	return c.readResultOK()
}

// HandleErrorPacket reads error message from ERR_Packet.
// Spec: https://dev.mysql.com/doc/internals/en/packet-ERR_Packet.html
func (c *ExtendedConn) HandleErrorPacket(data []byte) error {
	return c.handleErrorPacket(data)
}

// ResetSequence resets command sequence counter.
func (c *ExtendedConn) ResetSequence() {
	c.sequence = 0
}

// Close the connection.
func (c *ExtendedConn) Close() error {
	// Reset buffer length parameter
	// If it's not zero bad stuff happens
	c.buf.length = 0
	return c.mysqlConn.Close()
}
