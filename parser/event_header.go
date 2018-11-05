package parser

import (
	"errors"

	"github.com/localhots/pretty"
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

	eventBody []byte
}

// Spec: https://dev.mysql.com/doc/internals/en/event-header-fields.html
func (r *Reader) parseHeader(data []byte) (*EventHeader, error) {
	headerLen := r.headerLen()
	if len(data) < headerLen {
		return nil, ErrInvalidHeader
	}
	// pretty.Println(headerLen, data)

	buf := newReadBuffer(data)
	h := &EventHeader{
		Timestamp: buf.readUint32(),
		Type:      EventType(buf.readUint8()),
		ServerID:  buf.readUint32(),
		EventLen:  buf.readUint32(),
	}
	if r.format.Version == 0 || r.format.Version >= 3 {
		h.NextOffset = buf.readUint32()
		h.Flags = buf.readUint16()
	}
	if r.format.Version >= 4 {
		h.ExtraHeaders = buf.readStringVarLen(headerLen - 19)
	}
	h.eventBody = buf.cur()

	if h.NextOffset > 0 {
		r.state.Offset = uint64(h.NextOffset)
	}

	csa := r.format.ServerDetails.ChecksumAlgorithm
	if h.Type != FormatDescriptionEvent && csa == ChecksumAlgorithmCRC32 {
		h.eventBody = h.eventBody[:len(h.eventBody)-4]
	}

	// pretty.Println(h)

	switch h.Type {
	case FormatDescriptionEvent:
		r.format = decodeFormatDescription(h.eventBody)
		pretty.Println(h.Type.String(), r.format)
	case RotateEvent:
		r.state = r.decodeRotateEvent(h.eventBody)
		pretty.Println(h.Type.String(), r.state)
	case TableMapEvent:
		tm := r.decodeTableMap(h.eventBody)
		r.tableMap[tm.TableID] = tm
		// pretty.Println(h.Type.String(), tm)
	case WriteRowsEventV0, WriteRowsEventV1, WriteRowsEventV2,
		UpdateRowsEventV0, UpdateRowsEventV1, UpdateRowsEventV2,
		DeleteRowsEventV0, DeleteRowsEventV1, DeleteRowsEventV2:
		r.decodeRowsEvent(h.eventBody, h.Type)
	case XIDEvent, GTIDEvent:
		// TODO: Add support for these too
	case QueryEvent:
		// TODO: Handle schema changes
	}

	return h, nil
}

func (r *Reader) headerLen() int {
	const defaultHeaderLength = 19
	if r.format.EventHeaderLength > 0 {
		return int(r.format.EventHeaderLength)
	}
	return defaultHeaderLength
}
