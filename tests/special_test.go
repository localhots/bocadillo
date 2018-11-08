package tests

import (
	"testing"

	"github.com/localhots/bocadillo/mysql"
)

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
			suite.insertAndCompareExp(t, tbl, in, exp)
		})
	}
}
