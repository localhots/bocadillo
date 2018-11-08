package tests

import (
	"fmt"
	"testing"

	"github.com/localhots/bocadillo/mysql"
)

func TestNull(t *testing.T) {
	tbl := suite.createTable(mysql.ColumnTypeTiny, "", attrUnsigned|attrAllowNull)
	defer tbl.drop(t)

	uint8p := func(v uint8) *uint8 { return &v }
	for _, v := range []*uint8{uint8p(0), uint8p(1), nil} {
		strv := "NULL"
		if v != nil {
			strv = fmt.Sprint(*v)
		}
		t.Run(strv, func(t *testing.T) {
			suite.insertAndCompare(t, tbl, v)
		})
	}
}
