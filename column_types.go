package blt

import (
	"fmt"
)

type columnType byte

// Spec: https://dev.mysql.com/doc/internals/en/com-query-response.html#column-type
const (
	colTypeDecimal    columnType = 0x00
	colTypeTiny       columnType = 0x01
	colTypeShort      columnType = 0x02
	colTypeLong       columnType = 0x03
	colTypeFloat      columnType = 0x04
	colTypeDouble     columnType = 0x05
	colTypeNull       columnType = 0x06
	colTypeTimestamp  columnType = 0x07
	colTypeLonglong   columnType = 0x08
	colTypeInt24      columnType = 0x09
	colTypeDate       columnType = 0x0a
	colTypeTime       columnType = 0x0b
	colTypeDatetime   columnType = 0x0c
	colTypeYear       columnType = 0x0d
	colTypeNewDate    columnType = 0x0e // Internal
	colTypeVarchar    columnType = 0x0f
	colTypeBit        columnType = 0x10
	colTypeTimestamp2 columnType = 0x11 // Internal
	colTypeDatetime2  columnType = 0x12 // Internal
	colTypeTime2      columnType = 0x13 // Internal

	colTypeJSON       columnType = 0xF5
	colTypeNewDecimal columnType = 0xF6
	colTypeEnum       columnType = 0xF7
	colTypeSet        columnType = 0xF8
	colTypeTinyblob   columnType = 0xF9
	colTypeMediumblob columnType = 0xFA
	colTypeLongblob   columnType = 0xFB
	colTypeBlob       columnType = 0xFC
	colTypeVarstring  columnType = 0xFD
	colTypeString     columnType = 0xFE
	colTypeGeometry   columnType = 0xFF
)

func (ct columnType) String() string {
	switch ct {
	case colTypeDecimal:
		return "Decimal"
	case colTypeTiny:
		return "Tiny"
	case colTypeShort:
		return "Short"
	case colTypeLong:
		return "Long"
	case colTypeFloat:
		return "Float"
	case colTypeDouble:
		return "Double"
	case colTypeNull:
		return "Null"
	case colTypeTimestamp:
		return "Timestamp"
	case colTypeLonglong:
		return "Longlong"
	case colTypeInt24:
		return "Int24"
	case colTypeDate:
		return "Date"
	case colTypeTime:
		return "Time"
	case colTypeDatetime:
		return "Datetime"
	case colTypeYear:
		return "Year"
	case colTypeNewDate:
		return "NewDate"
	case colTypeVarchar:
		return "Varchar"
	case colTypeBit:
		return "Bit"
	case colTypeTimestamp2:
		return "Timestamp2"
	case colTypeDatetime2:
		return "Datetime2"
	case colTypeTime2:
		return "Time2"
	case colTypeJSON:
		return "JSON"
	case colTypeNewDecimal:
		return "NewDecimal"
	case colTypeEnum:
		return "Enum"
	case colTypeSet:
		return "Set"
	case colTypeTinyblob:
		return "Tinyblob"
	case colTypeMediumblob:
		return "Mediumblob"
	case colTypeLongblob:
		return "Longblob"
	case colTypeBlob:
		return "Blob"
	case colTypeVarstring:
		return "Varstring"
	case colTypeString:
		return "String"
	case colTypeGeometry:
		return "Geometry"
	default:
		return fmt.Sprintf("Unknown(%d)", ct)
	}
}
