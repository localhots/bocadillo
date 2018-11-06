package reader

import (
	"github.com/juju/errors"
	"github.com/localhots/bocadillo/binlog"
	"github.com/localhots/bocadillo/schema"
	"github.com/localhots/pretty"
)

// Reader ...
type Reader struct {
	conn     *SlaveConn
	state    binlog.Position
	format   binlog.FormatDescription
	tableMap map[uint64]binlog.TableDescription
	schema   *schema.Schema
}

// Event ...
type Event struct {
	Header binlog.EventHeader
	Body   []byte
}

// NewReader ...
func NewReader(conn *SlaveConn) (*Reader, error) {
	r := &Reader{
		conn:     conn,
		tableMap: make(map[uint64]binlog.TableDescription),
	}

	if err := conn.DisableChecksum(); err != nil {
		return nil, errors.Annotate(err, "disable binlog checksum")
	}
	if err := conn.RegisterSlave(); err != nil {
		return nil, errors.Annotate(err, "register slave server")
	}
	if err := conn.StartBinlogDump(); err != nil {
		return nil, errors.Annotate(err, "start binlog dump")
	}

	return r, nil
}

// ReadEvent ...
func (r *Reader) ReadEvent() (*Event, error) {
	connBuff, err := r.conn.ReadPacket()
	if err != nil {
		return nil, err
	}

	var evt Event
	if err := evt.Header.Decode(connBuff, r.format); err != nil {
		return nil, errors.Annotate(err, "decode event header")
	}

	if evt.Header.NextOffset > 0 {
		r.state.Offset = uint64(evt.Header.NextOffset)
	}

	evt.Body = connBuff[r.format.HeaderLen():]

	csa := r.format.ServerDetails.ChecksumAlgorithm
	if evt.Header.Type != binlog.EventTypeFormatDescription && csa == binlog.ChecksumAlgorithmCRC32 {
		evt.Body = evt.Body[:len(evt.Body)-4]
	}

	// pretty.Println(h)

	switch evt.Header.Type {
	case binlog.EventTypeFormatDescription:
		var fde binlog.FormatDescriptionEvent
		if err = fde.Decode(evt.Body); err == nil {
			r.format = fde.FormatDescription
		}
		pretty.Println(evt.Header.Type.String(), r.format)
	case binlog.EventTypeRotate:
		var re binlog.RotateEvent
		if err = re.Decode(evt.Body, r.format); err == nil {
			r.state = re.NextFile
		}
		pretty.Println(evt.Header.Type.String(), r.state)
	case binlog.EventTypeTableMap:
		var tme binlog.TableMapEvent
		if err = tme.Decode(evt.Body, r.format); err == nil {
			r.tableMap[tme.TableID] = tme.TableDescription
		}
		// pretty.Println(evt.Header.Type.String(), tm)
	case binlog.EventTypeWriteRowsV0,
		binlog.EventTypeWriteRowsV1,
		binlog.EventTypeWriteRowsV2,
		binlog.EventTypeUpdateRowsV0,
		binlog.EventTypeUpdateRowsV1,
		binlog.EventTypeUpdateRowsV2,
		binlog.EventTypeDeleteRowsV0,
		binlog.EventTypeDeleteRowsV1,
		binlog.EventTypeDeleteRowsV2:

		re := binlog.RowsEvent{Type: evt.Header.Type}
		tableID := re.PeekTableID(evt.Body, r.format)
		td, ok := r.tableMap[tableID]
		if !ok {
			return nil, errors.New("Unknown table ID")
		}
		if err = re.Decode(evt.Body, r.format, td); err == nil {
			pretty.Println(re)
		}
	case binlog.EventTypeXID:
		// TODO: Add support
	case binlog.EventTypeGTID:
		// TODO: Add support
	case binlog.EventTypeQuery:
		// TODO: Handle schema changes
	}

	return &evt, err
}
