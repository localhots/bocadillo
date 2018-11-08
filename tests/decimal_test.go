package tests

import (
	"strings"
	"testing"

	"github.com/localhots/bocadillo/mysql"
)

func TestDecimal(t *testing.T) {
	inputs := map[string][]string{
		"3,1": {
			"0.0",
			"1.0",
			"12.3",
			"10.1",
			"62.9",
			"50.1",
			"99.9",
		},
		"6,2": {
			"0.00",
			"1.00",
			"1.33",
			"10.16",
			"620.99",
			"5000.01",
			"9999.99",
		},
		"10,4": {
			"0.0000",
			"1.0001",
			"1.3301",
			"10.1600",
			"620.9999",
			"500000.0001",
			"999999.9999",
		},
		"30,10": {
			// NOTE: At certain length there's undesired zero fill :/
			"0000000000000000000.0000000000",
			"0000000000000000001.0000000001",
			"99999999999999999999.9999999999",
		},
	}

	for length, vals := range inputs {
		t.Run(length, func(t *testing.T) {
			tbl := suite.createTable(mysql.ColumnTypeDecimal, length, attrNone)
			defer tbl.drop(t)

			for _, v := range vals {
				t.Run(v, func(t *testing.T) {
					suite.insertAndCompare(t, tbl, v)
				})
				if !strings.HasPrefix(v, "0") {
					t.Run("-"+v, func(t *testing.T) {
						suite.insertAndCompare(t, tbl, "-"+v)
					})
				}
			}
		})
	}
}
