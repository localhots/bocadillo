package tests

import (
	"crypto/rand"
	"strconv"
	"testing"

	"github.com/localhots/bocadillo/mysql"
)

func TestTinyblob(t *testing.T) {
	tbl := suite.createTable(mysql.ColumnTypeTinyblob, "", attrNone)
	defer tbl.drop(t)

	for _, v := range nRandBytes(0, 1, 100, 255) {
		t.Run(strconv.Itoa(len(v)), func(t *testing.T) {
			suite.insertAndCompare(t, tbl, v)
		})
	}
}

func TestBlob(t *testing.T) {
	tbl := suite.createTable(mysql.ColumnTypeBlob, "", attrNone)
	defer tbl.drop(t)

	for _, v := range nRandBytes(0, 1, 10000, 65535) {
		t.Run(strconv.Itoa(len(v)), func(t *testing.T) {
			suite.insertAndCompare(t, tbl, v)
		})
	}
}

// func TestText(t *testing.T)
//
// Blob and Text values are encoded identically. It's best to handle it in the
// reader where schema is available and could tell how to encode these.
// Currently reader doesn't maintain schema and there's no way to test it.

// func TestMediumblob(t *testing.T)
// func TestLongblob(t *testing.T)
//
// Tried testing Mediumblob and Longblob the same way, got this error:
// Error 1105: Parameter of prepared statement which is set through
// mysql_send_long_data() is longer than 'max_allowed_packet' bytes
//
// That is from the client trying to insert a massive blob. I guess that's good
// enough and I hope these types work too.

func nRandBytes(ns ...int) [][]byte {
	nns := make([][]byte, len(ns))
	for i, n := range ns {
		nns[i] = randBytes(n)
	}
	return nns
}

func randBytes(n int) []byte {
	s := make([]byte, n)
	rand.Read(s)
	return s
}
