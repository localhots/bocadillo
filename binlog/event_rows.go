package binlog

import (
	"fmt"
	"time"

	"github.com/localhots/bocadillo/mysql"
	"github.com/localhots/bocadillo/tools"
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

	// pretty.Println(e.Type.String(), buf.Cur())

	e.Rows = make([][]interface{}, 0)
	for buf.More() {
		row, err := e.decodeRows(buf, td, e.ColumnBitmap1)
		if err != nil {
			return err
		}
		e.Rows = append(e.Rows, row)

		if RowsEventHasSecondBitmap(e.Type) {
			row, err := e.decodeRows(buf, td, e.ColumnBitmap2)
			if err != nil {
				return err
			}
			e.Rows = append(e.Rows, row)
		}
	}
	return nil
}

func (e *RowsEvent) decodeRows(buf *tools.Buffer, td TableDescription, bm []byte) ([]interface{}, error) {
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

		row[i] = e.decodeValue(buf, mysql.ColumnType(td.ColumnTypes[i]), td.ColumnMeta[i])
		// fmt.Printf("Parsed %s, meta %x, value %++v\n",
		// 	mysql.ColumnType(td.ColumnTypes[i]).String(), td.ColumnMeta[i], row[i],
		// )
	}
	return row, nil
}

func (e *RowsEvent) decodeValue(buf *tools.Buffer, ct mysql.ColumnType, meta uint16) interface{} {
	var length int
	if ct == mysql.ColumnTypeString {
		if meta > 0xFF {
			typeByte := uint8(meta >> 8)
			lengthByte := uint8(meta & 0xFF)
			if typeByte&0x30 != 0x30 {
				ct = mysql.ColumnType(typeByte | 0x30)
				length = int(uint16(lengthByte) | (uint16((typeByte&0x30)^0x30) << 4))
			} else {
				ct = mysql.ColumnType(typeByte)
				length = int(lengthByte)
			}
		} else {
			length = int(meta)
		}
	}

	switch ct {
	case mysql.ColumnTypeNull:
		return nil

	// Integer
	case mysql.ColumnTypeTiny:
		return buf.ReadUint8()
	case mysql.ColumnTypeShort:
		return buf.ReadUint16()
	case mysql.ColumnTypeInt24:
		return buf.ReadUint24()
	case mysql.ColumnTypeLong:
		return buf.ReadUint32()
	case mysql.ColumnTypeLonglong:
		return buf.ReadUint64()

	// Float
	case mysql.ColumnTypeFloat:
		return buf.ReadFloat32()
	case mysql.ColumnTypeDouble:
		return buf.ReadFloat64()

	// Decimals
	case mysql.ColumnTypeNewDecimal:
		precision := int(meta >> 8)
		decimals := int(meta & 0xFF)
		dec, n := mysql.DecodeDecimal(buf.Cur(), precision, decimals)
		buf.Skip(n)
		return dec

	// Date and Time
	case mysql.ColumnTypeYear:
		return mysql.DecodeYear(buf.ReadUint8())
	case mysql.ColumnTypeDate:
		return mysql.DecodeDate(buf.ReadUint24())
	case mysql.ColumnTypeTime:
		return mysql.DecodeTime(buf.ReadUint24())
	case mysql.ColumnTypeTime2:
		v, n := mysql.DecodeTime2(buf.Cur(), meta)
		buf.Skip(n)
		return v
	case mysql.ColumnTypeTimestamp:
		ts := buf.ReadUint32()
		return mysql.FracTime{Time: time.Unix(int64(ts), 0)}.String()
	case mysql.ColumnTypeTimestamp2:
		v, n := mysql.DecodeTimestamp2(buf.Cur(), meta)
		buf.Skip(n)
		return v
	case mysql.ColumnTypeDatetime:
		return mysql.DecodeDatetime(buf.ReadUint64())
	case mysql.ColumnTypeDatetime2:
		v, n := mysql.DecodeDatetime2(buf.Cur(), meta)
		buf.Skip(n)
		return v

	// Strings
	case mysql.ColumnTypeString:
		return readString(buf, length)
	case mysql.ColumnTypeVarchar, mysql.ColumnTypeVarstring:
		return readString(buf, int(meta))

	// Blobs
	case mysql.ColumnTypeBlob, mysql.ColumnTypeGeometry:
		return buf.ReadStringVarEnc(int(meta))
	case mysql.ColumnTypeJSON:
		jdata := buf.ReadStringVarEnc(int(meta))
		rawj, _ := mysql.DecodeJSON(jdata)
		return rawj
	case mysql.ColumnTypeTinyblob:
		return buf.ReadStringVarEnc(1)
	case mysql.ColumnTypeMediumblob:
		return buf.ReadStringVarEnc(3)
	case mysql.ColumnTypeLongblob:
		return buf.ReadStringVarEnc(4)

	// Bits
	case mysql.ColumnTypeBit:
		nbits := int(((meta >> 8) * 8) + (meta & 0xFF))
		length = int(nbits+7) / 8
		return mysql.DecodeBit(buf.Cur(), nbits, length)
	case mysql.ColumnTypeSet:
		length = int(meta & 0xFF)
		nbits := length * 8
		return mysql.DecodeBit(buf.Cur(), nbits, length)

	// Stuff
	case mysql.ColumnTypeEnum:
		return buf.ReadVarLen64(int(meta & 0xFF))

	// Unsupported
	case mysql.ColumnTypeDecimal:
		// Old decimal
		fallthrough
	case mysql.ColumnTypeNewDate:
		// Too new
		fallthrough
	default:
		return fmt.Errorf("unsupported type: %d (%s) %x %x", ct, ct.String(), meta, buf.Cur())
	}
}

// FIXME: Something is fishy with this whole string decoding. It seems like it
// could be simplified greatly
func readString(buf *tools.Buffer, length int) string {
	if length < 256 {
		return string(buf.ReadStringVarEnc(1))
	}
	return string(buf.ReadStringVarEnc(2))
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
