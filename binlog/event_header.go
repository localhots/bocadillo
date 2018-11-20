package binlog

import (
	"errors"

	"github.com/localhots/bocadillo/buffer"
)

var (
	// ErrInvalidHeader is returned when event header cannot be parsed.
	ErrInvalidHeader = errors.New("Header is invalid")
)

// EventHeader represents binlog event header.
type EventHeader struct {
	Timestamp    uint32
	Type         EventType
	ServerID     uint32
	EventLen     uint32
	NextOffset   uint32
	Flags        uint16
	ExtraHeaders []byte
}

// Decode decodes given buffer into event header.
// Spec: https://dev.mysql.com/doc/internals/en/event-header-fields.html
func (h *EventHeader) Decode(connBuff []byte, fd FormatDescription) error {
	headerLen := fd.HeaderLen()
	if len(connBuff) < headerLen {
		return ErrInvalidHeader
	}

	buf := buffer.New(connBuff)
	h.Timestamp = buf.ReadUint32()
	h.Type = EventType(buf.ReadUint8())
	h.ServerID = buf.ReadUint32()
	h.EventLen = buf.ReadUint32()

	if fd.Version == 0 || fd.Version >= 3 {
		h.NextOffset = buf.ReadUint32()
		h.Flags = buf.ReadUint16()
	}
	if fd.Version >= 4 {
		h.ExtraHeaders = buf.ReadStringVarLen(headerLen - 19)
	}

	return nil
}
