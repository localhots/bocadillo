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
		"0000-00-00",
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
		"0000-00-00T00:00:00Z",
		// This is the lowest I could get
		// Spec says 1970-01-01 00:00:01 should be supported
		"1970-01-01T01:00:01Z",
		"1975-01-01T00:00:01Z",
		"1985-01-01T00:00:01Z",
		"1999-12-31T23:59:59Z",
		"2018-11-08T19:26:00Z",
		"2038-01-19T03:14:07Z",
		"2038-01-19T04:14:07Z", // Should be outside supported range? 2038-01-19 03:14:07
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
			"0000-00-00T00:00:00Z",
			"1000-01-01T00:00:00Z",
			"1975-01-01T00:00:01Z",
			"1985-01-01T00:00:01Z",
			"1999-12-31T23:59:59Z",
			"2018-11-08T19:26:00Z",
			"2038-01-19T03:14:07Z",
			"9999-12-31T23:59:59Z",
		},
		"1": {
			"0000-00-00T00:00:00.0Z",
			"1000-01-01T00:00:00.1Z",
			"1975-01-01T00:00:01.1Z",
			"1985-01-01T00:00:01.1Z",
			"1999-12-31T23:59:59.1Z",
			"2018-11-08T19:26:00.1Z",
			"2038-01-19T03:14:07.1Z",
			"9999-12-31T23:59:59.1Z",
		},
		"2": {
			"0000-00-00T00:00:00.00Z",
			"1000-01-01T00:00:00.22Z",
			"1975-01-01T00:00:01.22Z",
			"1985-01-01T00:00:01.22Z",
			"1999-12-31T23:59:59.22Z",
			"2018-11-08T19:26:00.22Z",
			"2038-01-19T03:14:07.22Z",
			"9999-12-31T23:59:59.22Z",
		},
		"3": {
			"0000-00-00T00:00:00.000Z",
			"1000-01-01T00:00:00.333Z",
			"1975-01-01T00:00:01.333Z",
			"1985-01-01T00:00:01.333Z",
			"1999-12-31T23:59:59.333Z",
			"2018-11-08T19:26:00.333Z",
			"2038-01-19T03:14:07.333Z",
			"9999-12-31T23:59:59.333Z",
		},
		"4": {
			"0000-00-00T00:00:00.0000Z",
			"1000-01-01T00:00:00.4444Z",
			"1975-01-01T00:00:01.4444Z",
			"1985-01-01T00:00:01.4444Z",
			"1999-12-31T23:59:59.4444Z",
			"2018-11-08T19:26:00.4444Z",
			"2038-01-19T03:14:07.4444Z",
			"9999-12-31T23:59:59.4444Z",
		},
		"5": {
			"0000-00-00T00:00:00.00000Z",
			"1000-01-01T00:00:00.55555Z",
			"1975-01-01T00:00:01.55555Z",
			"1985-01-01T00:00:01.55555Z",
			"1999-12-31T23:59:59.55555Z",
			"2018-11-08T19:26:00.55555Z",
			"2038-01-19T03:14:07.55555Z",
			"9999-12-31T23:59:59.55555Z",
		},
		"6": {
			"0000-00-00T00:00:00.000000Z",
			"1000-01-01T00:00:00.666666Z",
			"1975-01-01T00:00:01.666666Z",
			"1985-01-01T00:00:01.666666Z",
			"1999-12-31T23:59:59.666666Z",
			"2018-11-08T19:26:00.666666Z",
			"2038-01-19T03:14:07.666666Z",
			"9999-12-31T23:59:59.666666Z",
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
