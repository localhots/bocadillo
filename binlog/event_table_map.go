package binlog

import (
	"github.com/localhots/blt/mysql"
	"github.com/localhots/blt/tools"
)

// TableDescription contains table details required to process rows events.
type TableDescription struct {
	Flags       uint16
	SchemaName  string
	TableName   string
	ColumnCount uint64
	ColumnTypes []byte
	ColumnMeta  []uint16
	NullBitmask []byte
}

// TableMapEvent contains table description alongside an ID that would be used
// to reference the table in the following rows events.
type TableMapEvent struct {
	TableID uint64
	TableDescription
}

// Decode decodes given buffer into a table map event.
// Spec: https://dev.mysql.com/doc/internals/en/table-map-event.html
func (e *TableMapEvent) Decode(connBuff []byte, fd FormatDescription) error {
	buf := tools.NewBuffer(connBuff)
	idSize := fd.TableIDSize(EventTypeTableMap)
	if idSize == 6 {
		e.TableID = buf.ReadUint48()
	} else {
		e.TableID = uint64(buf.ReadUint32())
	}

	e.Flags = buf.ReadUint16()
	schemaName, _ := buf.ReadStringLenEnc()
	e.SchemaName = string(schemaName)
	buf.Skip(1) // Always 0x00
	tableName, _ := buf.ReadStringLenEnc()
	e.TableName = string(tableName)
	buf.Skip(1) // Always 0x00
	e.ColumnCount, _, _ = buf.ReadUintLenEnc()
	e.ColumnTypes = buf.ReadStringVarLen(int(e.ColumnCount))
	colMeta, _ := buf.ReadStringLenEnc()
	e.ColumnMeta = decodeColumnMeta(colMeta, e.ColumnTypes)
	e.NullBitmask = buf.ReadStringVarLen(int(e.ColumnCount+8) / 7)

	return nil
}

func decodeColumnMeta(data []byte, cols []byte) []uint16 {
	pos := 0
	meta := make([]uint16, len(cols))
	for i, typ := range cols {
		switch mysql.ColumnType(typ) {
		case mysql.ColumnTypeString,
			mysql.ColumnTypeNewDecimal:

			// TODO: Is that correct?
			meta[i] = uint16(data[pos])<<8 | uint16(data[pos+1])
			pos += 2
		case mysql.ColumnTypeVarchar,
			mysql.ColumnTypeVarstring,
			mysql.ColumnTypeBit:

			// TODO: Is that correct?
			meta[i] = mysql.DecodeUint16(data[pos:])
			pos += 2
		case mysql.ColumnTypeFloat,
			mysql.ColumnTypeDouble,
			mysql.ColumnTypeBlob,
			mysql.ColumnTypeGeometry,
			mysql.ColumnTypeJSON,
			mysql.ColumnTypeTime2,
			mysql.ColumnTypeDatetime2,
			mysql.ColumnTypeTimestamp2:

			meta[i] = uint16(data[pos])
			pos++
		}
	}
	return meta
}
