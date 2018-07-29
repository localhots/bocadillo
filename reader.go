package blt

import (
	"context"
	"database/sql/driver"
	"fmt"
	"io"
	"os"

	"github.com/juju/errors"
	"github.com/localhots/gobelt/log"
	"github.com/localhots/mysql"
)

// Reader ...
type Reader struct {
	conn     *mysql.ExtendedConn
	conf     Config
	state    Position
	format   FormatDescription
	tableMap map[uint64]TableMap
}

// Config ...
type Config struct {
	ServerID uint32
	File     string
	Offset   uint32
	Hostname string
}

// Position ...
type Position struct {
	File   string
	Offset uint64
}

const (
	// Bytes
	resultOK  byte = 0x00
	resultEOF byte = 0xFE
	resultERR byte = 0xFF
)

// NewReader ...
func NewReader(conn driver.Conn, conf Config) (*Reader, error) {
	if conf.Hostname == "" {
		name, err := os.Hostname()
		if err != nil {
			return nil, err
		}
		conf.Hostname = name
	}

	extconn, err := mysql.ExtendConn(conn)
	if err != nil {
		return nil, err
	}
	r := &Reader{
		conn:     extconn,
		conf:     conf,
		tableMap: make(map[uint64]TableMap),
	}

	if err := r.disableChecksum(); err != nil {
		return nil, errors.Annotate(err, "Failed to disable binlog checksum")
	}
	if err := r.registerSlave(); err != nil {
		return nil, errors.Annotate(err, "Failed to register slave server")
	}
	if err := r.binlogDump(); err != nil {
		return nil, errors.Annotate(err, "Failed to start binlog dump")
	}

	return r, nil
}

// Connect ...
func Connect(dsn string, conf Config) (*Reader, error) {
	conn, err := (&mysql.MySQLDriver{}).Open(dsn)
	if err != nil {
		return nil, err
	}
	return NewReader(conn, conf)
}

// ReadEventHeader reads next event from the log and decodes its header. Header
// is then used to decode the event.
func (r *Reader) ReadEventHeader(ctx context.Context) (*EventHeader, error) {
	data, err := r.conn.ReadPacket()
	if err != nil {
		return nil, err
	}
	switch data[0] {
	case resultOK:
		return r.parseHeader(data[1:])
	case resultERR:
		return nil, r.conn.HandleErrorPacket(data)
	case resultEOF:
		log.Debug(ctx, "EOF received")
		return nil, nil
	default:
		log.Errorf(ctx, "Unexpected header: %x", data[0])
		return nil, nil
	}
}

// Spec: https://dev.mysql.com/doc/internals/en/com-register-slave.html
func (r *Reader) registerSlave() error {
	const comRegisterSlave byte = 21
	r.conn.ResetSequence()

	buf := newCommandBuffer(1 + 4 + 1 + len(r.conf.Hostname) + 1 + 1 + 2 + 4 + 4)
	buf.writeByte(comRegisterSlave)
	buf.writeUint32(r.conf.ServerID)
	buf.writeString(r.conf.Hostname)
	// The rest of the payload would be zeroes, consider following code for
	// reference:
	//
	// buf.writeString(username)
	// buf.writeString(password)
	// buf.writeUint16(port)
	// buf.writeUint32(replicationRank)
	// buf.writeUint32(masterID)

	return r.runCmd(buf)
}

// Spec: https://dev.mysql.com/doc/internals/en/com-binlog-dump.html
// TODO: https://dev.mysql.com/doc/internals/en/com-binlog-dump-gtid.html
func (r *Reader) binlogDump() error {
	const comBinlogDump byte = 18
	r.conn.ResetSequence()

	r.state.File = r.conf.File
	r.state.Offset = uint64(r.conf.Offset)
	// First event offset is 4
	if r.state.Offset < 4 {
		r.state.Offset = 4
	}

	buf := newCommandBuffer(1 + 4 + 2 + 4 + len(r.state.File))
	buf.writeByte(comBinlogDump)
	buf.writeUint32(uint32(r.state.Offset))
	buf.skip(2) // Flags
	buf.writeUint32(r.conf.ServerID)
	buf.writeStringEOF(r.state.File)

	return r.runCmd(buf)
}

func (r *Reader) runCmd(buf *buffer) error {
	err := r.conn.WritePacket(buf.data)
	if err != nil {
		return err
	}
	return r.conn.ReadResultOK()
}

func (r *Reader) disableChecksum() error {
	cs, err := r.getVar("BINLOG_CHECKSUM")
	if err != nil {
		return err
	}

	if cs != "NONE" {
		return r.setVar("@master_binlog_checksum", "NONE")
	}
	return nil
}

func (r *Reader) getVar(name string) (string, error) {
	rows, err := r.conn.Query(fmt.Sprintf("SHOW VARIABLES LIKE %q", name), []driver.Value{})
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

func (r *Reader) setVar(name, val string) error {
	return r.conn.Exec(fmt.Sprintf("SET %s=%q", name, val))
}

func notEOF(err error) error {
	if err == io.EOF {
		return nil
	}
	return err
}
