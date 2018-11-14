package reader

import (
	"github.com/juju/errors"
	"github.com/localhots/bocadillo/binlog"
	"github.com/localhots/bocadillo/reader/slave"
)

// Reader is a binary log reader.
type Reader struct {
	conn     *slave.Conn
	state    binlog.Position
	format   binlog.FormatDescription
	tableMap map[uint64]binlog.TableDescription
}

// Event contains binlog event details.
type Event struct {
	Format binlog.FormatDescription
	Header binlog.EventHeader
	Buffer []byte
	Offset uint64

	// Table is not empty for rows events
	Table *binlog.TableDescription
}

var (
	// ErrUnknownTableID is returned when a table ID from a rows event is
	// missing in the table map index.
	ErrUnknownTableID = errors.New("Unknown table ID")
)

// New creates a new binary log reader.
func New(dsn string, sc slave.Config) (*Reader, error) {
	conn, err := slave.Connect(dsn, sc)
	if err != nil {
		return nil, errors.Annotate(err, "establish slave connection")
	}

	r := &Reader{
		conn: conn,
		state: binlog.Position{
			File:   sc.File,
			Offset: uint64(sc.Offset),
		},
	}
	r.initTableMap()

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

// ReadEvent reads next event from the binary log.
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
		if err := fde.Decode(evt.Buffer); err != nil {
			return nil, errors.Annotate(err, "decode format description event")
		}
		r.format = fde.FormatDescription
		evt.Format = fde.FormatDescription

	case binlog.EventTypeRotate:
		var re binlog.RotateEvent
		if err := re.Decode(evt.Buffer, r.format); err != nil {
			return nil, errors.Annotate(err, "decode rotate event")
		}
		r.state = re.NextFile

	case binlog.EventTypeTableMap:
		var tme binlog.TableMapEvent
		if err := tme.Decode(evt.Buffer, r.format); err != nil {
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
		tableID, flags := re.PeekTableIDAndFlags(evt.Buffer, r.format)
		td, ok := r.tableMap[tableID]
		if !ok {
			return nil, ErrUnknownTableID
		}
		evt.Table = &td

		// Throttle table map clearing. This flag could be part of every single
		// rows event
		if binlog.RowsFlagEndOfStatement&flags > 0 && len(r.tableMap) > 100 {
			// Clear table map
			r.initTableMap()
		}
	case binlog.EventTypeQuery:
		// Can be decoded by the receiver
	case binlog.EventTypeXID:
		// Can be decoded by the receiver
	case binlog.EventTypeGTID:
		// TODO: Add support
	}

	return &evt, err
}

// State returns current position in the binary log.
func (r *Reader) State() binlog.Position {
	return r.state
}

// Close underlying database connection.
func (r *Reader) Close() error {
	return r.conn.Close()
}

func (r *Reader) initTableMap() {
	r.tableMap = make(map[uint64]binlog.TableDescription)
}

// DecodeRows decodes buffer into a rows event.
func (e Event) DecodeRows() (binlog.RowsEvent, error) {
	re := binlog.RowsEvent{Type: e.Header.Type}
	if binlog.RowsEventVersion(e.Header.Type) < 0 {
		return re, errors.New("invalid rows event")
	}
	err := re.Decode(e.Buffer, e.Format, *e.Table)
	return re, err
}
