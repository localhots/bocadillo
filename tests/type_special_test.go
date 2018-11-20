package tests

import (
	"testing"

	"github.com/localhots/bocadillo/mysql"
)

func TestJSON(t *testing.T) {
	tbl := suite.createTable(mysql.ColumnTypeJSON, "", attrNone)
	defer tbl.drop(t)

	exp := []byte(`{"hello": "world", "foo": [1, 2, 3.75]}`)
	suite.insertAndCompare(t, tbl, exp)
}

func TestGeometry(t *testing.T) {
	// Geometry values are tricky
	// FIXME: Implement me one day
	t.Skip("Not implemented")
}

func TestBit(t *testing.T) {
	// Where to get these?
	// FIXME: Implement me one day
	t.Skip("Not implemented")
}

func TestSet(t *testing.T) {
	tbl := suite.createTable(mysql.ColumnTypeSet, "'a', 'b', 'c'", attrNone)
	defer tbl.drop(t)

	const (
		// TODO: How do I define such a bitmask properly?
		bA int64 = 1
		bB int64 = 2
		bC int64 = 4
	)

	inputs := map[string]int64{
		"":      0,
		"a":     bA,
		"a,b":   bA | bB,
		"a,c":   bA | bC,
		"a,b,c": bA | bB | bC,
	}

	for in, exp := range inputs {
		t.Run("input "+in, func(t *testing.T) {
			suite.insertAndCompareExp(t, tbl, iSlice(in), iSlice(exp))
		})
	}
}

func TestEnum(t *testing.T) {
	tbl := suite.createTable(mysql.ColumnTypeEnum, "'a', 'b', 'c'", attrNone)
	defer tbl.drop(t)

	inputs := map[string]int64{
		"":  0,
		"a": 1,
		"b": 2,
		"c": 3,
	}
	for in, exp := range inputs {
		t.Run("input "+in, func(t *testing.T) {
			suite.insertAndCompareExp(t, tbl, iSlice(in), iSlice(exp))
		})
	}
}

func iSlice(i interface{}) []interface{} {
	return []interface{}{i}
}
