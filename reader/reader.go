package reader

import (
	"github.com/juju/errors"
	"github.com/localhots/bocadillo/binlog"
	"github.com/localhots/bocadillo/reader/schema"
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
	Format binlog.FormatDescription
	Header binlog.EventHeader
	Buffer []byte

	// Table is not empty for rows events
	Table *binlog.TableDescription
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
		return nil, errors.Annotate(err, "read next event")
	}

	evt := Event{Format: r.format}
	if err := evt.Header.Decode(connBuff, r.format); err != nil {
		return nil, errors.Annotate(err, "decode event header")
	}

	if evt.Header.NextOffset > 0 {
		r.state.Offset = uint64(evt.Header.NextOffset)
	}

	evt.Buffer = connBuff[r.format.HeaderLen():]
	csa := r.format.ServerDetails.ChecksumAlgorithm
	if evt.Header.Type != binlog.EventTypeFormatDescription && csa == binlog.ChecksumAlgorithmCRC32 {
		// Remove trailing CRC32 checksum, we're not going to verify it
		evt.Buffer = evt.Buffer[:len(evt.Buffer)-4]
	}

	switch evt.Header.Type {
	case binlog.EventTypeFormatDescription:
		var fde binlog.FormatDescriptionEvent
		err = fde.Decode(evt.Buffer)
		if err != nil {
			return nil, errors.Annotate(err, "decode format description event")
		}
		r.format = fde.FormatDescription
		evt.Format = fde.FormatDescription
	case binlog.EventTypeRotate:
		var re binlog.RotateEvent
		err = re.Decode(evt.Buffer, r.format)
		if err != nil {
			return nil, errors.Annotate(err, "decode rotate event")
		}
		r.state = re.NextFile
	case binlog.EventTypeTableMap:
		var tme binlog.TableMapEvent
		err = tme.Decode(evt.Buffer, r.format)
		if err != nil {
			return nil, errors.Annotate(err, "decode table map event")
		}
		r.tableMap[tme.TableID] = tme.TableDescription
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
		tableID := re.PeekTableID(evt.Buffer, r.format)
		td, ok := r.tableMap[tableID]
		if !ok {
			return nil, errors.New("Unknown table ID")
		}
		evt.Table = &td
	case binlog.EventTypeQuery:
		// Can be decoded by the receiver
	case binlog.EventTypeXID:
		// Can be decoded by the receiver
	case binlog.EventTypeGTID:
		// TODO: Add support
	}

	return &evt, err
}
