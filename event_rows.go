package blt

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

	pretty.Println(typ.String(), rows, tm, buf.cur())
}
