package binlog

import (
	"github.com/localhots/bocadillo/mysql"
	"github.com/localhots/bocadillo/tools"
	"github.com/localhots/pretty"
)

// RowsEvent contains a Rows Event.
type RowsEvent struct {
	Type          EventType
	TableID       uint64
	Flags         uint16
	ExtraData     []byte
	ColumnCount   uint64
	ColumnBitmap1 []byte
	ColumnBitmap2 []byte
	Rows          [][]interface{}
}

type rowsFlag uint16

const (
	rowsFlagEndOfStatement     rowsFlag = 0x0001
	rowsFlagNoForeignKeyChecks rowsFlag = 0x0002
	rowsFlagNoUniqueKeyChecks  rowsFlag = 0x0004
	rowsFlagRowHasColumns      rowsFlag = 0x0008

	freeTableMapID = 0x00FFFFFF
)

// PeekTableID returns table ID without decoding whole event.
func (e *RowsEvent) PeekTableID(connBuff []byte, fd FormatDescription) uint64 {
	if fd.TableIDSize(e.Type) == 6 {
		return mysql.DecodeUint48(connBuff)
	}
	return uint64(mysql.DecodeUint32(connBuff))
}

// Decode decodes given buffer into a rows event event.
func (e *RowsEvent) Decode(connBuff []byte, fd FormatDescription, td TableDescription) error {
	// pretty.Println(data)
	buf := tools.NewBuffer(connBuff)
	idSize := fd.TableIDSize(e.Type)
	if idSize == 6 {
		e.TableID = buf.ReadUint48()
	} else {
		e.TableID = uint64(buf.ReadUint32())
	}

	e.Flags = buf.ReadUint16()

	if RowsEventHasExtraData(e.Type) {
		// Extra data length is part of extra data, deduct 2 bytes as they
		// already store its length
		extraLen := buf.ReadUint16() - 2
		e.ExtraData = buf.ReadStringVarLen(int(extraLen))
	}

	e.ColumnCount, _, _ = buf.ReadUintLenEnc()
	e.ColumnBitmap1 = buf.ReadStringVarLen(int(e.ColumnCount+7) / 8)
	if RowsEventHasSecondBitmap(e.Type) {
		e.ColumnBitmap2 = buf.ReadStringVarLen(int(e.ColumnCount+7) / 8)
	}

	pretty.Println(e.Type.String(), e, td, buf.Cur())

	e.decodeRows(buf, td, e.ColumnBitmap1)
	return nil
}

func (e *RowsEvent) decodeRows(buf *tools.Buffer, td TableDescription, bm []byte) {
	count := 0
	for i := 0; i < int(e.ColumnCount); i++ {
		if isBitSet(bm, i) {
			count++
		}
	}
	count = (count + 7) / 8

	nullBM := buf.ReadStringVarLen(count)
	nullIdx := 0
	row := make([]interface{}, e.ColumnCount)

	var err error
	for i := 0; i < int(e.ColumnCount); i++ {
		if !isBitSet(bm, i) {
			continue
		}

		isNull := (uint32(nullBM[nullIdx/8]) >> uint32(nullIdx%8)) & 1
		nullIdx++
		if isNull > 0 {
			row[i] = nil
			continue
		}

		row[i], err = e.decodeValue(buf, mysql.ColumnType(td.ColumnTypes[i]), td.ColumnMeta[i])

		if err != nil {
			panic(err)
		}
	}
}

func (e *RowsEvent) decodeValue(buf *tools.Buffer, ct mysql.ColumnType, meta uint16) (interface{}, error) {
	switch ct {
	case mysql.ColumnTypeDecimal:
		pretty.Println("Type", ct.String())
	case mysql.ColumnTypeTiny:
		pretty.Println("Type", ct.String())
	case mysql.ColumnTypeShort:
		pretty.Println("Type", ct.String())
	case mysql.ColumnTypeLong:
		pretty.Println("Type", ct.String())
	case mysql.ColumnTypeFloat:
		pretty.Println("Type", ct.String())
	case mysql.ColumnTypeDouble:
		pretty.Println("Type", ct.String())
	case mysql.ColumnTypeNull:
		pretty.Println("Type", ct.String())
	case mysql.ColumnTypeTimestamp:
		pretty.Println("Type", ct.String())
	case mysql.ColumnTypeLonglong:
		pretty.Println("Type", ct.String())
	case mysql.ColumnTypeInt24:
		pretty.Println("Type", ct.String())
	case mysql.ColumnTypeDate:
		pretty.Println("Type", ct.String())
	case mysql.ColumnTypeTime:
		pretty.Println("Type", ct.String())
	case mysql.ColumnTypeDatetime:
		pretty.Println("Type", ct.String())
	case mysql.ColumnTypeYear:
		pretty.Println("Type", ct.String())
	case mysql.ColumnTypeVarchar:
		pretty.Println("Type", ct.String())
	case mysql.ColumnTypeBit:
		pretty.Println("Type", ct.String())

	case mysql.ColumnTypeJSON:
		pretty.Println("Type", ct.String())
	case mysql.ColumnTypeNewDecimal:
		pretty.Println("Type", ct.String())
	case mysql.ColumnTypeEnum:
		pretty.Println("Type", ct.String())
	case mysql.ColumnTypeSet:
		pretty.Println("Type", ct.String())
	case mysql.ColumnTypeTinyblob:
		pretty.Println("Type", ct.String())
	case mysql.ColumnTypeMediumblob:
		pretty.Println("Type", ct.String())
	case mysql.ColumnTypeLongblob:
		pretty.Println("Type", ct.String())
	case mysql.ColumnTypeBlob:
		pretty.Println("Type", ct.String())
	case mysql.ColumnTypeVarstring:
		pretty.Println("Type", ct.String())
	case mysql.ColumnTypeString:
		pretty.Println("Type", ct.String())
	case mysql.ColumnTypeGeometry:
		pretty.Println("Type", ct.String())
	}
	return nil, nil
}

func isBitSet(bm []byte, i int) bool {
	return bm[i>>3]&(1<<(uint(i)&7)) > 0
}

// RowsEventVersion returns rows event versions. If event is not a rows type -1
// is returned.
func RowsEventVersion(et EventType) int {
	switch et {
	case EventTypeWriteRowsV0, EventTypeUpdateRowsV0, EventTypeDeleteRowsV0:
		return 0
	case EventTypeWriteRowsV1, EventTypeUpdateRowsV1, EventTypeDeleteRowsV1:
		return 1
	case EventTypeWriteRowsV2, EventTypeUpdateRowsV2, EventTypeDeleteRowsV2:
		return 2
	default:
		return -1
	}
}

// RowsEventHasExtraData returns true if given event is of rows type and
// contains extra data.
func RowsEventHasExtraData(et EventType) bool {
	return RowsEventVersion(et) == 2
}

// RowsEventHasSecondBitmap returns true if given event is of rows type and
// contains a second bitmap.
func RowsEventHasSecondBitmap(et EventType) bool {
	switch et {
	case EventTypeUpdateRowsV1, EventTypeUpdateRowsV2:
		return true
	default:
		return false
	}
}
