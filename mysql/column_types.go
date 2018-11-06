package mysql

import (
	"fmt"
)

// ColumnType represents MySQL column type.
type ColumnType byte

// Spec: https://dev.mysql.com/doc/internals/en/com-query-response.html#column-type
const (
	ColumnTypeDecimal    ColumnType = 0x00
	ColumnTypeTiny       ColumnType = 0x01
	ColumnTypeShort      ColumnType = 0x02
	ColumnTypeLong       ColumnType = 0x03
	ColumnTypeFloat      ColumnType = 0x04
	ColumnTypeDouble     ColumnType = 0x05
	ColumnTypeNull       ColumnType = 0x06
	ColumnTypeTimestamp  ColumnType = 0x07
	ColumnTypeLonglong   ColumnType = 0x08
	ColumnTypeInt24      ColumnType = 0x09
	ColumnTypeDate       ColumnType = 0x0a
	ColumnTypeTime       ColumnType = 0x0b
	ColumnTypeDatetime   ColumnType = 0x0c
	ColumnTypeYear       ColumnType = 0x0d
	ColumnTypeNewDate    ColumnType = 0x0e // Internal
	ColumnTypeVarchar    ColumnType = 0x0f
	ColumnTypeBit        ColumnType = 0x10
	ColumnTypeTimestamp2 ColumnType = 0x11 // Internal
	ColumnTypeDatetime2  ColumnType = 0x12 // Internal
	ColumnTypeTime2      ColumnType = 0x13 // Internal

	ColumnTypeJSON       ColumnType = 0xF5
	ColumnTypeNewDecimal ColumnType = 0xF6
	ColumnTypeEnum       ColumnType = 0xF7
	ColumnTypeSet        ColumnType = 0xF8
	ColumnTypeTinyblob   ColumnType = 0xF9
	ColumnTypeMediumblob ColumnType = 0xFA
	ColumnTypeLongblob   ColumnType = 0xFB
	ColumnTypeBlob       ColumnType = 0xFC
	ColumnTypeVarstring  ColumnType = 0xFD
	ColumnTypeString     ColumnType = 0xFE
	ColumnTypeGeometry   ColumnType = 0xFF
)

func (ct ColumnType) String() string {
	switch ct {
	case ColumnTypeDecimal:
		return "Decimal"
	case ColumnTypeTiny:
		return "Tiny"
	case ColumnTypeShort:
		return "Short"
	case ColumnTypeLong:
		return "Long"
	case ColumnTypeFloat:
		return "Float"
	case ColumnTypeDouble:
		return "Double"
	case ColumnTypeNull:
		return "Null"
	case ColumnTypeTimestamp:
		return "Timestamp"
	case ColumnTypeLonglong:
		return "Longlong"
	case ColumnTypeInt24:
		return "Int24"
	case ColumnTypeDate:
		return "Date"
	case ColumnTypeTime:
		return "Time"
	case ColumnTypeDatetime:
		return "Datetime"
	case ColumnTypeYear:
		return "Year"
	case ColumnTypeNewDate:
		return "NewDate"
	case ColumnTypeVarchar:
		return "Varchar"
	case ColumnTypeBit:
		return "Bit"
	case ColumnTypeTimestamp2:
		return "Timestamp2"
	case ColumnTypeDatetime2:
		return "Datetime2"
	case ColumnTypeTime2:
		return "Time2"
	case ColumnTypeJSON:
		return "JSON"
	case ColumnTypeNewDecimal:
		return "NewDecimal"
	case ColumnTypeEnum:
		return "Enum"
	case ColumnTypeSet:
		return "Set"
	case ColumnTypeTinyblob:
		return "Tinyblob"
	case ColumnTypeMediumblob:
		return "Mediumblob"
	case ColumnTypeLongblob:
		return "Longblob"
	case ColumnTypeBlob:
		return "Blob"
	case ColumnTypeVarstring:
		return "Varstring"
	case ColumnTypeString:
		return "String"
	case ColumnTypeGeometry:
		return "Geometry"
	default:
		return fmt.Sprintf("Unknown(%d)", ct)
	}
}
