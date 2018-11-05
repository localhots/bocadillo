package parser

import (
	"fmt"

	"github.com/localhots/pretty"
)

// Rows contains a Rows Event.
type Rows struct {
	EventType     EventType
	TableID       uint64
	Flags         uint16
	ExtraData     []byte
	ColumnCount   uint64
	ColumnBitmap1 []byte
	ColumnBitmap2 []byte
	Rows          [][]interface{}
	TableMap      *TableMap
}

type rowsFlag uint16

const (
	rowsFlagEndOfStatement     rowsFlag = 0x0001
	rowsFlagNoForeignKeyChecks rowsFlag = 0x0002
	rowsFlagNoUniqueKeyChecks  rowsFlag = 0x0004
	rowsFlagRowHasColumns      rowsFlag = 0x0008

	freeTableMapID = 0x00FFFFFF
)

func (r *Reader) decodeRowsEvent(data []byte, typ EventType) {
	// pretty.Println(data)
	buf := newReadBuffer(data)
	rows := Rows{EventType: typ}
	idSize := r.format.tableIDSize(typ)
	if idSize == 6 {
		rows.TableID = buf.readUint48()
	} else {
		rows.TableID = uint64(buf.readUint32())
	}

	rows.Flags = buf.readUint16()

	if typ.isEither(WriteRowsEventV2, UpdateRowsEventV2, DeleteRowsEventV2) {
		// Extra data length is part of extra data, deduct 2 bytes as they
		// already store its length
		extraLen := buf.readUint16() - 2
		rows.ExtraData = buf.readStringVarLen(int(extraLen))
	}

	rows.ColumnCount, _ = buf.readUintLenEnc()
	rows.ColumnBitmap1 = buf.readStringVarLen(int(rows.ColumnCount+7) / 8)
	if typ.isEither(UpdateRowsEventV2, UpdateRowsEventV1) {
		rows.ColumnBitmap2 = buf.readStringVarLen(int(rows.ColumnCount+7) / 8)
	}

	tm, ok := r.tableMap[rows.TableID]
	if !ok {
		panic(fmt.Errorf("Out of sync: no table map definition for ID=%d", rows.TableID))
	}
	rows.TableMap = &tm

	pretty.Println(typ.String(), rows, tm, buf.cur())

	rows.decodeRows(buf, rows.ColumnBitmap1)
}

func (r *Rows) decodeRows(buf *buffer, bm []byte) {
	count := 0
	for i := 0; i < int(r.ColumnCount); i++ {
		if isBitSet(bm, i) {
			count++
		}
	}
	count = (count + 7) / 8

	nullBM := buf.readStringVarLen(count)
	nullCnt := 0
	row := make([]interface{}, r.ColumnCount)

	pretty.Println(count, nullBM)

	var err error
	for i := 0; i < int(r.ColumnCount); i++ {
		if !isBitSet(bm, i) {
			continue
		}

		isNull := (uint32(nullBM[nullCnt/8]) >> uint32(nullCnt%8)) & 0x01
		nullCnt++
		if isNull > 0 {
			row[i] = nil
			continue
		}

		row[i], err = r.decodeValue(buf, columnType(r.TableMap.ColumnTypes[i]), r.TableMap.ColumnMeta[i])

		if err != nil {
			panic(err)
		}
	}
}

func (r *Rows) decodeValue(buf *buffer, ct columnType, meta uint16) (interface{}, error) {
	switch ct {
	case colTypeDecimal:
		pretty.Println("Type", ct.String())
	case colTypeTiny:
		pretty.Println("Type", ct.String())
	case colTypeShort:
		pretty.Println("Type", ct.String())
	case colTypeLong:
		pretty.Println("Type", ct.String())
	case colTypeFloat:
		pretty.Println("Type", ct.String())
	case colTypeDouble:
		pretty.Println("Type", ct.String())
	case colTypeNull:
		pretty.Println("Type", ct.String())
	case colTypeTimestamp:
		pretty.Println("Type", ct.String())
	case colTypeLonglong:
		pretty.Println("Type", ct.String())
	case colTypeInt24:
		pretty.Println("Type", ct.String())
	case colTypeDate:
		pretty.Println("Type", ct.String())
	case colTypeTime:
		pretty.Println("Type", ct.String())
	case colTypeDatetime:
		pretty.Println("Type", ct.String())
	case colTypeYear:
		pretty.Println("Type", ct.String())
	case colTypeVarchar:
		pretty.Println("Type", ct.String())
	case colTypeBit:
		pretty.Println("Type", ct.String())

	case colTypeJSON:
		pretty.Println("Type", ct.String())
	case colTypeNewDecimal:
		pretty.Println("Type", ct.String())
	case colTypeEnum:
		pretty.Println("Type", ct.String())
	case colTypeSet:
		pretty.Println("Type", ct.String())
	case colTypeTinyblob:
		pretty.Println("Type", ct.String())
	case colTypeMediumblob:
		pretty.Println("Type", ct.String())
	case colTypeLongblob:
		pretty.Println("Type", ct.String())
	case colTypeBlob:
		pretty.Println("Type", ct.String())
	case colTypeVarstring:
		pretty.Println("Type", ct.String())
	case colTypeString:
		pretty.Println("Type", ct.String())
	case colTypeGeometry:
		pretty.Println("Type", ct.String())
	}
	return nil, nil
}

func isBitSet(bm []byte, i int) bool {
	return bm[i>>3]&(1<<(uint(i)&7)) > 0
}
