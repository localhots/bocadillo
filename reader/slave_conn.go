package reader

import (
	"database/sql/driver"
	"encoding/hex"
	"fmt"
	"io"
	"os"

	"github.com/localhots/blt/tools"
	"github.com/localhots/mysql"
)

// SlaveConn ...
type SlaveConn struct {
	conn *mysql.ExtendedConn
	conf Config
}

// Config ...
type Config struct {
	ServerID uint32
	File     string
	Offset   uint32
	Hostname string
}

const (
	// Commands
	comRegisterSlave byte = 21
	comBinlogDump    byte = 18

	// Bytes
	resultOK  byte = 0x00
	resultEOF byte = 0xFE
	resultERR byte = 0xFF
)

// Connect ...
func Connect(dsn string, conf Config) (*SlaveConn, error) {
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

	conn, err := (&mysql.MySQLDriver{}).Open(dsn)
	if err != nil {
		return nil, err
	}

	extconn, err := mysql.ExtendConn(conn)
	if err != nil {
		return nil, err
	}

	return &SlaveConn{conn: extconn, conf: conf}, nil
}

// ReadPacket reads next packet from the server and processes the first status
// byte.
func (c *SlaveConn) ReadPacket() ([]byte, error) {
	data, err := c.conn.ReadPacket()
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
func (c *SlaveConn) RegisterSlave() error {
	c.conn.ResetSequence()

	buf := tools.NewCommandBuffer(1 + 4 + 1 + len(c.conf.Hostname) + 1 + 1 + 2 + 4 + 4)
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

	fmt.Println(hex.Dump(buf.Bytes()))
	return c.runCmd(buf.Bytes())
}

// StartBinlogDump issues a BINLOG_DUMP command to master.
// Spec: https://dev.mysql.com/doc/internals/en/com-binlog-dump.html
// TODO: https://dev.mysql.com/doc/internals/en/com-binlog-dump-gtid.html
func (c *SlaveConn) StartBinlogDump() error {
	c.conn.ResetSequence()

	buf := tools.NewCommandBuffer(1 + 4 + 2 + 4 + len(c.conf.File))
	buf.WriteByte(comBinlogDump)
	buf.WriteUint32(uint32(c.conf.Offset))
	buf.Skip(2) // Flags
	buf.WriteUint32(c.conf.ServerID)
	buf.WriteStringEOF(c.conf.File)

	return c.runCmd(buf.Bytes())
}

// DisableChecksum disables CRC32 checksums for this connection.
func (c *SlaveConn) DisableChecksum() error {
	cs, err := c.GetVar("BINLOG_CHECKSUM")
	if err != nil {
		return err
	}

	if cs != "NONE" {
		return c.SetVar("@master_binlog_checksum", "NONE")
	}
	return nil
}

// GetVar fetches value of the given variable.
func (c *SlaveConn) GetVar(name string) (string, error) {
	rows, err := c.conn.Query(fmt.Sprintf("SHOW VARIABLES LIKE %q", name), []driver.Value{})
	if err != nil {
		return "", notEOF(err)
	}
	defer rows.Close()

	res := make([]driver.Value, len(rows.Columns()))
	err = rows.Next(res)
	if err != nil {
		return "", notEOF(err)
	}

	return string(res[1].([]byte)), nil
}

// SetVar assigns a new value to the given variable.
func (c *SlaveConn) SetVar(name, val string) error {
	return c.conn.Exec(fmt.Sprintf("SET %s=%q", name, val))
}

func (c *SlaveConn) runCmd(data []byte) error {
	err := c.conn.WritePacket(data)
	if err != nil {
		return err
	}
	return c.conn.ReadResultOK()
}

func notEOF(err error) error {
	if err == io.EOF {
		return nil
	}
	return err
}
