package binlog

import (
	"github.com/localhots/bocadillo/tools"
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
	buf := tools.NewBuffer(connBuff)

	e.SlaveProxyID = buf.ReadUint32()
	e.ExecutionTime = buf.ReadUint32()
	schemaLen := int(buf.ReadUint8())
	e.ErrorCode = buf.ReadUint16()
	statusVarLen := int(buf.ReadUint8())
	copy(e.StatusVars, buf.Read(statusVarLen))
	copy(e.Schema, buf.Read(schemaLen))
	buf.Skip(1) // Always 0x00
	copy(e.Query, buf.Cur())
}
