package tests

import (
	"fmt"
	"testing"

	"github.com/localhots/bocadillo/mysql"
)

//
// Tiny
//

func TestTinyUnsigned(t *testing.T) {
	tbl := suite.createTable(mysql.ColumnTypeTiny, "", attrUnsigned)
	defer tbl.drop(t)

	for _, v := range []uint8{0, 1, 123, 200, 255} {
		t.Run(fmt.Sprint(v), func(t *testing.T) {
			suite.insertAndCompare(t, tbl, v)
		})
	}
}

func TestTinySigned(t *testing.T) {
	tbl := suite.createTable(mysql.ColumnTypeTiny, "", attrNone)
	defer tbl.drop(t)

	for _, v := range []int8{-128, -1, 0, 1, 127} {
		t.Run(fmt.Sprint(v), func(t *testing.T) {
			suite.insertAndCompare(t, tbl, v)
		})
	}
}

//
// Short
//

func TestShortUnsigned(t *testing.T) {
	tbl := suite.createTable(mysql.ColumnTypeShort, "", attrUnsigned)
	defer tbl.drop(t)

	for _, v := range []uint16{0, 1, 123, 20000, 65535} {
		t.Run(fmt.Sprint(v), func(t *testing.T) {
			suite.insertAndCompare(t, tbl, v)
		})
	}
}

func TestShortSigned(t *testing.T) {
	tbl := suite.createTable(mysql.ColumnTypeShort, "", attrNone)
	defer tbl.drop(t)

	for _, v := range []int16{-32768, -1, 0, 1, 32767} {
		t.Run(fmt.Sprint(v), func(t *testing.T) {
			suite.insertAndCompare(t, tbl, v)
		})
	}
}

//
// Int24
//

func TestInt24Unsigned(t *testing.T) {
	tbl := suite.createTable(mysql.ColumnTypeInt24, "", attrUnsigned)
	defer tbl.drop(t)

	for _, v := range []uint32{0, 1, 123, 20000, 16777215} {
		t.Run(fmt.Sprint(v), func(t *testing.T) {
			suite.insertAndCompare(t, tbl, v)
		})
	}
}

func TestInt24Signed(t *testing.T) {
	tbl := suite.createTable(mysql.ColumnTypeInt24, "", attrNone)
	defer tbl.drop(t)

	for _, v := range []int32{-8388608, -1, 0, 1, 8388607} {
		t.Run(fmt.Sprint(v), func(t *testing.T) {
			suite.insertAndCompare(t, tbl, v)
		})
	}
}

//
// Long
//

func TestLongUnsigned(t *testing.T) {
	tbl := suite.createTable(mysql.ColumnTypeLong, "", attrUnsigned)
	defer tbl.drop(t)

	for _, v := range []uint32{0, 1, 123, 200000000, 4294967295} {
		t.Run(fmt.Sprint(v), func(t *testing.T) {
			suite.insertAndCompare(t, tbl, v)
		})
	}
}

func TestLongSigned(t *testing.T) {
	tbl := suite.createTable(mysql.ColumnTypeLong, "", attrNone)
	defer tbl.drop(t)

	for _, v := range []int32{-2147483648, -1, 0, 1, 2147483647} {
		t.Run(fmt.Sprint(v), func(t *testing.T) {
			suite.insertAndCompare(t, tbl, v)
		})
	}
}

//
// Longlong
//

func TestLonglongUnsigned(t *testing.T) {
	tbl := suite.createTable(mysql.ColumnTypeLonglong, "", attrUnsigned)
	defer tbl.drop(t)

	for _, v := range []uint64{0, 1, 123, 200000000, 18446744073709551615} {
		t.Run(fmt.Sprint(v), func(t *testing.T) {
			suite.insertAndCompare(t, tbl, v)
		})
	}
}

func TestLonglongSigned(t *testing.T) {
	tbl := suite.createTable(mysql.ColumnTypeLonglong, "", attrNone)
	defer tbl.drop(t)

	for _, v := range []int64{-9223372036854775808, -1, 0, 1, 9223372036854775807} {
		t.Run(fmt.Sprint(v), func(t *testing.T) {
			suite.insertAndCompare(t, tbl, v)
		})
	}
}
