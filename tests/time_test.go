package tests

import (
	"fmt"
	"testing"

	"github.com/localhots/bocadillo/mysql"
)

func TestYear(t *testing.T) {
	tbl := suite.createTable(mysql.ColumnTypeYear, "", attrNone)
	defer tbl.drop(t)

	for _, v := range []int16{1900, 1901, 1999, 2018, 2155} {
		t.Run(fmt.Sprint(v), func(t *testing.T) {
			suite.insertAndCompare(t, tbl, v)
		})
	}
}

func TestDate(t *testing.T) {
	tbl := suite.createTable(mysql.ColumnTypeDate, "", attrNone)
	defer tbl.drop(t)

	vals := []string{
		"1000-01-01",
		"1234-05-06",
		"1500-01-01",
		"2016-09-08",
		"2030-06-12",
		"9999-12-31",
	}
	for _, v := range vals {
		t.Run(v, func(t *testing.T) {
			suite.insertAndCompare(t, tbl, v)
		})
	}
}

func TestTime(t *testing.T) {
	tbl := suite.createTable(mysql.ColumnTypeTime, "", attrNone)
	defer tbl.drop(t)

	vals := []string{
		"00:00:00",
		"06:00:00",
		"11:11:11",
		"12:34:56",
		"24:00:00",
		"23:59:59",
	}
	for _, v := range vals {
		t.Run(v, func(t *testing.T) {
			suite.insertAndCompare(t, tbl, v)
		})
	}
}

func TestTimestamp(t *testing.T) {
	tbl := suite.createTable(mysql.ColumnTypeTimestamp, "", attrNone)
	defer tbl.drop(t)

	vals := []string{
		// This is the lowest I could get
		// Spec says 1970-01-01 00:00:01 should be supported
		"1970-01-01 01:00:01",
		"1975-01-01 00:00:01",
		"1985-01-01 00:00:01",
		"1999-12-31 23:59:59",
		"2018-11-08 19:26:00",
		"2038-01-19 03:14:07",
		"2038-01-19 04:14:07", // Should be outside supported range? 2038-01-19 03:14:07
	}
	for _, v := range vals {
		t.Run(v, func(t *testing.T) {
			suite.insertAndCompare(t, tbl, v)
		})
	}
}

func TestDatetime(t *testing.T) {
	inputs := map[string][]string{
		"0": {
			"1000-01-01 00:00:00",
			"1975-01-01 00:00:01",
			"1985-01-01 00:00:01",
			"1999-12-31 23:59:59",
			"2018-11-08 19:26:00",
			"2038-01-19 03:14:07",
			"9999-12-31 23:59:59",
		},
		"1": {
			"1000-01-01 00:00:00.1",
			"1975-01-01 00:00:01.1",
			"1985-01-01 00:00:01.1",
			"1999-12-31 23:59:59.1",
			"2018-11-08 19:26:00.1",
			"2038-01-19 03:14:07.1",
			"9999-12-31 23:59:59.1",
		},
		"2": {
			"1000-01-01 00:00:00.22",
			"1975-01-01 00:00:01.22",
			"1985-01-01 00:00:01.22",
			"1999-12-31 23:59:59.22",
			"2018-11-08 19:26:00.22",
			"2038-01-19 03:14:07.22",
			"9999-12-31 23:59:59.22",
		},
		"3": {
			"1000-01-01 00:00:00.333",
			"1975-01-01 00:00:01.333",
			"1985-01-01 00:00:01.333",
			"1999-12-31 23:59:59.333",
			"2018-11-08 19:26:00.333",
			"2038-01-19 03:14:07.333",
			"9999-12-31 23:59:59.333",
		},
		"4": {
			"1000-01-01 00:00:00.4444",
			"1975-01-01 00:00:01.4444",
			"1985-01-01 00:00:01.4444",
			"1999-12-31 23:59:59.4444",
			"2018-11-08 19:26:00.4444",
			"2038-01-19 03:14:07.4444",
			"9999-12-31 23:59:59.4444",
		},
		"5": {
			"1000-01-01 00:00:00.55555",
			"1975-01-01 00:00:01.55555",
			"1985-01-01 00:00:01.55555",
			"1999-12-31 23:59:59.55555",
			"2018-11-08 19:26:00.55555",
			"2038-01-19 03:14:07.55555",
			"9999-12-31 23:59:59.55555",
		},
		"6": {
			"1000-01-01 00:00:00.666666",
			"1975-01-01 00:00:01.666666",
			"1985-01-01 00:00:01.666666",
			"1999-12-31 23:59:59.666666",
			"2018-11-08 19:26:00.666666",
			"2038-01-19 03:14:07.666666",
			"9999-12-31 23:59:59.666666",
		},
	}
	for length, vals := range inputs {
		t.Run(length, func(t *testing.T) {
			tbl := suite.createTable(mysql.ColumnTypeDatetime, length, attrNone)
			defer tbl.drop(t)

			for _, v := range vals {
				t.Run(v, func(t *testing.T) {
					suite.insertAndCompare(t, tbl, v)
				})
			}
		})
	}
}
