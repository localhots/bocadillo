package binlog

import (
	"github.com/localhots/bocadillo/buffer"
)

// QueryEvent contains query details.
type QueryEvent struct {
	SlaveProxyID  uint32
	ExecutionTime uint32
	ErrorCode     uint16
	StatusVars    []byte
	Schema        []byte
	Query         []byte
}

// Decode given buffer into a qeury event.
// Spec: https://dev.mysql.com/doc/internals/en/query-event.html
func (e *QueryEvent) Decode(connBuff []byte) {
	buf := buffer.New(connBuff)

	e.SlaveProxyID = buf.ReadUint32()
	e.ExecutionTime = buf.ReadUint32()
	schemaLen := int(buf.ReadUint8())
	e.ErrorCode = buf.ReadUint16()
	statusVarLen := int(buf.ReadUint8())

	e.StatusVars = make([]byte, statusVarLen)
	copy(e.StatusVars, buf.Read(statusVarLen))

	// FIXME: This is not by the spec but seem to work
	// It could be there's an error somewhere and this byte skipping corrects it
	buf.Skip(1) // Always 0x00
	e.Schema = make([]byte, schemaLen)
	copy(e.Schema, buf.Read(schemaLen))

	buf.Skip(1) // Always 0x00
	e.Query = buf.Cur()
}
