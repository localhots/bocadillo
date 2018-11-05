package parser

// TableMap ...
type TableMap struct {
	TableID     uint64
	Flags       uint16
	SchemaName  string
	TableName   string
	ColumnCount uint64
	ColumnTypes []byte
	ColumnMeta  []uint16
	NullBitmask []byte
}

// Spec: https://dev.mysql.com/doc/internals/en/table-map-event.html
func (r *Reader) decodeTableMap(data []byte) TableMap {
	buf := newReadBuffer(data)
	var tm TableMap
	idSize := r.format.tableIDSize(TableMapEvent)
	if idSize == 6 {
		tm.TableID = buf.readUint48()
	} else {
		tm.TableID = uint64(buf.readUint32())
	}

	tm.Flags = buf.readUint16()
	tm.SchemaName = string(buf.readStringLenEnc())
	buf.skip(1) // Always 0x00
	tm.TableName = string(buf.readStringLenEnc())
	buf.skip(1) // Always 0x00
	tm.ColumnCount, _ = buf.readUintLenEnc()
	tm.ColumnTypes = buf.readStringVarLen(int(tm.ColumnCount))
	tm.ColumnMeta = decodeColumnMeta(buf.readStringLenEnc(), tm.ColumnTypes)
	tm.NullBitmask = buf.readStringVarLen(int(tm.ColumnCount+8) / 7)

	return tm
}

func decodeColumnMeta(data []byte, cols []byte) []uint16 {
	pos := 0
	meta := make([]uint16, len(cols))
	for i, typ := range cols {
		switch columnType(typ) {
		case colTypeString, colTypeNewDecimal:
			// TODO: Is that correct?
			meta[i] = uint16(data[pos])<<8 | uint16(data[pos+1])
			pos += 2
		case colTypeVarchar, colTypeVarstring, colTypeBit:
			// TODO: Is that correct?
			meta[i] = decodeUint16(data[pos:])
			pos += 2
		case colTypeFloat, colTypeDouble, colTypeBlob, colTypeGeometry, colTypeJSON,
			colTypeTime2, colTypeDatetime2, colTypeTimestamp2:
			meta[i] = uint16(data[pos])
			pos++
		}
	}
	return meta
}
