package tests

import (
	"fmt"
	"strings"
	"testing"

	"github.com/localhots/bocadillo/mysql"
)

func TestStrings(t *testing.T) {
	inputs := map[string][]string{}
	for _, length := range []int{1, 10, 100, 255} {
		inputs[fmt.Sprint(length)] = []string{
			strings.Repeat("a", length),
			strings.Repeat("1", length),
			strings.Repeat("!", length),
			// TODO: Support multi-byte and other encodings
		}
	}

	for _, ct := range []mysql.ColumnType{mysql.ColumnTypeString, mysql.ColumnTypeVarchar} {
		t.Run(ct.String(), func(t *testing.T) {
			for length, vals := range inputs {
				t.Run(length, func(t *testing.T) {
					tbl := suite.createTable(ct, length, attrNone)
					defer tbl.drop(t)

					for _, v := range vals {
						t.Run(fmt.Sprintf("%s x%d", v[:1], len(v)), func(t *testing.T) {
							suite.insertAndCompare(t, tbl, v)
						})
					}
				})
			}
		})
	}
}
