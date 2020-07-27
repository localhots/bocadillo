package driver

import (
	"context"
	"fmt"
	"os"

	"github.com/localhots/bocadillo/buffer"
	"github.com/localhots/bocadillo/mysql/driver/internal/mysql"
)

// Conn is a connection used to issue a binlog dump command.
type Conn struct {
	conn *mysql.ExtendedConn
	conf Config
}

// Config contains all the details necessary to establish a replica connection.
type Config struct {
	// File and offset describe current state.
	// File is the name of the binary log file.
	File string
	// Offset is the binary offset of the first event in the binary log file,
	// a starting point at which processing should begin.
	Offset uint32
	// ServerID should be a unique replica server identifier (i guess).
	ServerID uint32
	// Hostname along with server ID is used to identify the replica server
	// connection.
	Hostname string
}

const (
	// Commands
	comRegisterSlave byte = 21
	comBinlogDump    byte = 18

	// Result codes
	resultOK  byte = 0x00
	resultEOF byte = 0xFE
	resultERR byte = 0xFF
)

// Connect esablishes a new database connection. It is a go-sql-driver
// connection with a few low level functions exposed and with a high level
// wrapper that allows to execute just a few commands that are required for
// operation.
func Connect(dsn string, conf Config) (*Conn, error) {
	if conf.Hostname == "" {
		name, err := os.Hostname()
		if err != nil {
			return nil, err
		}
		conf.Hostname = name
	}
	conf.Hostname = "localhost"
	if conf.Offset == 0 {
		conf.Offset = 4
	}

	conn, err := (mysql.MySQLDriver{}).Open(dsn)
	if err != nil {
		return nil, err
	}

	extconn, err := mysql.ExtendConn(conn)
	if err != nil {
		return nil, err
	}

	return &Conn{conn: extconn, conf: conf}, nil
}

// ReadPacket reads next packet from the server and peeks at the status byte.
func (c *Conn) ReadPacket(ctx context.Context) ([]byte, error) {
	data, err := c.conn.ReadPacket(ctx)
	if err != nil {
		return nil, err
	}

	switch data[0] {
	case resultOK:
		return data[1:], nil
	case resultERR:
		return nil, c.conn.HandleErrorPacket(data)
	case resultEOF:
		return nil, nil
	default:
		return nil, fmt.Errorf("unexpected header: %x", data[0])
	}
}

// RegisterSlave issues a REGISTER_SLAVE command to master.
// Spec: https://dev.mysql.com/doc/internals/en/com-register-slave.html
func (c *Conn) RegisterSlave() error {
	c.conn.ResetSequence()

	buf := buffer.NewCommandBuffer(1 + 4 + 1 + len(c.conf.Hostname) + 1 + 1 + 2 + 4 + 4)
	buf.WriteByte(comRegisterSlave)
	buf.WriteUint32(c.conf.ServerID)
	buf.WriteStringLenEnc(c.conf.Hostname)
	// The rest of the payload would be zeroes, consider following code for
	// reference:
	//
	// buf.WriteStringLenEnc(username)
	// buf.WriteStringLenEnc(password)
	// buf.WriteUint16(port)
	// buf.WriteUint32(replicationRank)
	// buf.WriteUint32(masterID)

	return c.runCmd(buf.Bytes())
}

// StartBinlogDump issues a BINLOG_DUMP command to master.
// Spec: https://dev.mysql.com/doc/internals/en/com-binlog-dump.html
// TODO: https://dev.mysql.com/doc/internals/en/com-binlog-dump-gtid.html
func (c *Conn) StartBinlogDump() error {
	c.conn.ResetSequence()

	buf := buffer.NewCommandBuffer(1 + 4 + 2 + 4 + len(c.conf.File))
	buf.WriteByte(comBinlogDump)
	buf.WriteUint32(uint32(c.conf.Offset))
	buf.Skip(2) // Flags
	buf.WriteUint32(c.conf.ServerID)
	buf.WriteStringEOF(c.conf.File)

	return c.runCmd(buf.Bytes())
}

// DisableChecksum disables CRC32 checksums for this connection.
func (c *Conn) DisableChecksum() error {
	return c.SetVar("@master_binlog_checksum", "NONE")
}

// SetVar assigns a new value to the given variable.
func (c *Conn) SetVar(name, val string) error {
	return c.conn.Exec(fmt.Sprintf("SET %s=%q", name, val))
}

// Close the connection.
func (c *Conn) Close() error {
	return c.conn.Close()
}

func (c *Conn) runCmd(data []byte) error {
	err := c.conn.WritePacket(data)
	if err != nil {
		return err
	}
	return c.conn.ReadResultOK()
}
