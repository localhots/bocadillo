package reader

import (
	"context"
	"database/sql"

	_ "github.com/localhots/mysql" // MySQL driver

	"github.com/juju/errors"
	"github.com/localhots/bocadillo/binlog"
	"github.com/localhots/bocadillo/mysql"
	"github.com/localhots/bocadillo/reader/schema"
	"github.com/localhots/bocadillo/reader/slave"
)

// EnhancedReader is an extended version of the reader that maintains schema
// details to add column names and signed integers support.
type EnhancedReader struct {
	reader    *Reader
	safepoint binlog.Position
	schemaMgr *schema.Manager
}

// EnhancedRowsEvent ...
type EnhancedRowsEvent struct {
	Header binlog.EventHeader
	Table  binlog.TableDescription
	Rows   []map[string]interface{}
}

// NewEnhanced creates a new enhanced binary log reader.
func NewEnhanced(dsn string, sc slave.Config) (*EnhancedReader, error) {
	r, err := New(dsn, sc)
	if err != nil {
		return nil, err
	}

	conn, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}

	return &EnhancedReader{
		reader:    r,
		schemaMgr: schema.NewManager(conn),
		safepoint: r.state,
	}, nil
}

// WhitelistTables adds given tables of the given database to processing white
// list.
func (r *EnhancedReader) WhitelistTables(database string, tables ...string) error {
	for _, tbl := range tables {
		if err := r.schemaMgr.Manage(database, tbl); err != nil {
			return err
		}
	}
	return nil
}

// ReadEvent reads next event from the binary log.
func (r *EnhancedReader) ReadEvent() (*Event, error) {
	evt, err := r.reader.ReadEvent()
	if err != nil {
		return nil, err
	}

	switch evt.Header.Type {
	case binlog.EventTypeQuery:
		var qe binlog.QueryEvent
		qe.Decode(evt.Buffer)
		err = r.schemaMgr.ProcessQuery(string(qe.Schema), string(qe.Query))
	}

	return evt, err
}

// NextRowsEvent returns the next rows event for a whitelisted table. It blocks
// until next event is received or context is cancelled.
func (r *EnhancedReader) NextRowsEvent(ctx context.Context) (*EnhancedRowsEvent, error) {
	evtch := make(chan *EnhancedRowsEvent)
	errch := make(chan error)
	go func() {
		evt, err := r.nextRowsEvent()
		if err != nil {
			errch <- err
		} else {
			evtch <- evt
		}
	}()

	select {
	case evt := <-evtch:
		return evt, nil
	case err := <-errch:
		return nil, err
	case <-ctx.Done():
		return nil, nil
	}
}

func (r *EnhancedReader) nextRowsEvent() (*EnhancedRowsEvent, error) {
	for {
		evt, err := r.reader.ReadEvent()
		if err != nil {
			return nil, err
		}

		// Check if it's a rows event
		if binlog.RowsEventVersion(evt.Header.Type) < 0 {
			// fmt.Println("Not a rows event", evt.Header.Type.String())
			continue
		}

		tbl := r.schemaMgr.Schema.Table(evt.Table.SchemaName, evt.Table.TableName)
		if tbl == nil {
			// Not whitelisted
			continue
		}

		re, err := evt.DecodeRows()
		if err != nil {
			return nil, err
		}

		ere := EnhancedRowsEvent{
			Header: evt.Header,
			Table:  *evt.Table,
			Rows:   make([]map[string]interface{}, len(re.Rows)),
		}
		for i, row := range re.Rows {
			erow := make(map[string]interface{}, len(row))
			for j, val := range row {
				col := tbl.Column(j)
				if col == nil {
					return nil, errors.New("column index undefined")
				}
				ct := mysql.ColumnType(evt.Table.ColumnTypes[j])
				if !col.Unsigned {
					val = signNumber(val, ct)
				}
				erow[col.Name] = val
			}
			ere.Rows[i] = erow
		}

		return &ere, nil
	}
}

func (r *EnhancedReader) processEvent(evt Event) {
	switch evt.Header.Type {
	case binlog.EventTypeFormatDescription, binlog.EventTypeTableMap, binlog.EventTypeXID:
		r.safepoint.Offset = evt.Offset
	case binlog.EventTypeRotate:
		r.safepoint = r.reader.state
	}
}

// State returns current position in the binary log.
func (r *EnhancedReader) State() binlog.Position {
	return r.reader.state
}

// Safepoint returns last encountered position that is considered safe to start
// with.
func (r *EnhancedReader) Safepoint() binlog.Position {
	return r.safepoint
}

// Close underlying database connection.
func (r *EnhancedReader) Close() error {
	return r.reader.Close()
}

func signNumber(val interface{}, ct mysql.ColumnType) interface{} {
	switch tval := val.(type) {
	case uint8:
		return mysql.SignUint8(tval)
	case uint16:
		return mysql.SignUint16(tval)
	case uint32:
		if ct == mysql.ColumnTypeInt24 {
			return mysql.SignUint24(tval)
		}
		return mysql.SignUint32(tval)
	case uint64:
		return mysql.SignUint64(tval)
	default:
		return val
	}
}
