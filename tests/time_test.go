package tests

import (
	"fmt"
	"testing"
	"time"

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

	vals := []time.Time{
		parseTime("0000-00-00T00:00:00Z"),
		// This is the lowest I could get
		// Spec says 1970-01-01 00:00:01 should be supported
		parseTime("1970-01-01T01:00:01Z"),
		parseTime("1975-01-01T00:00:01Z"),
		parseTime("1985-01-01T00:00:01Z"),
		parseTime("1999-12-31T23:59:59Z"),
		parseTime("2018-11-08T19:26:00Z"),
		parseTime("2038-01-19T03:14:07Z"),
		parseTime("2038-01-19T04:14:07Z"), // Should be outside supported range? 2038-01-19 03:14:07
	}
	for _, v := range vals {
		t.Run(v.Format(time.RFC3339Nano), func(t *testing.T) {
			suite.insertAndCompare(t, tbl, v)
		})
	}
}

func TestDatetime(t *testing.T) {
	inputs := map[string][]time.Time{
		"0": {
			parseTime("0000-00-00T00:00:00Z"),
			parseTime("1000-01-01T00:00:00Z"),
			parseTime("1975-01-01T00:00:01Z"),
			parseTime("1985-01-01T00:00:01Z"),
			parseTime("1999-12-31T23:59:59Z"),
			parseTime("2018-11-08T19:26:00Z"),
			parseTime("2038-01-19T03:14:07Z"),
			parseTime("9999-12-31T23:59:59Z"),
		},
		"1": {
			parseTime("0000-00-00T00:00:00.0Z"),
			parseTime("1000-01-01T00:00:00.1Z"),
			parseTime("1975-01-01T00:00:01.1Z"),
			parseTime("1985-01-01T00:00:01.1Z"),
			parseTime("1999-12-31T23:59:59.1Z"),
			parseTime("2018-11-08T19:26:00.1Z"),
			parseTime("2038-01-19T03:14:07.1Z"),
			parseTime("9999-12-31T23:59:59.1Z"),
		},
		"2": {
			parseTime("0000-00-00T00:00:00.00Z"),
			parseTime("1000-01-01T00:00:00.22Z"),
			parseTime("1975-01-01T00:00:01.22Z"),
			parseTime("1985-01-01T00:00:01.22Z"),
			parseTime("1999-12-31T23:59:59.22Z"),
			parseTime("2018-11-08T19:26:00.22Z"),
			parseTime("2038-01-19T03:14:07.22Z"),
			parseTime("9999-12-31T23:59:59.22Z"),
		},
		"3": {
			parseTime("0000-00-00T00:00:00.000Z"),
			parseTime("1000-01-01T00:00:00.333Z"),
			parseTime("1975-01-01T00:00:01.333Z"),
			parseTime("1985-01-01T00:00:01.333Z"),
			parseTime("1999-12-31T23:59:59.333Z"),
			parseTime("2018-11-08T19:26:00.333Z"),
			parseTime("2038-01-19T03:14:07.333Z"),
			parseTime("9999-12-31T23:59:59.333Z"),
		},
		"4": {
			parseTime("0000-00-00T00:00:00.0000Z"),
			parseTime("1000-01-01T00:00:00.4444Z"),
			parseTime("1975-01-01T00:00:01.4444Z"),
			parseTime("1985-01-01T00:00:01.4444Z"),
			parseTime("1999-12-31T23:59:59.4444Z"),
			parseTime("2018-11-08T19:26:00.4444Z"),
			parseTime("2038-01-19T03:14:07.4444Z"),
			parseTime("9999-12-31T23:59:59.4444Z"),
		},
		"5": {
			parseTime("0000-00-00T00:00:00.00000Z"),
			parseTime("1000-01-01T00:00:00.55555Z"),
			parseTime("1975-01-01T00:00:01.55555Z"),
			parseTime("1985-01-01T00:00:01.55555Z"),
			parseTime("1999-12-31T23:59:59.55555Z"),
			parseTime("2018-11-08T19:26:00.55555Z"),
			parseTime("2038-01-19T03:14:07.55555Z"),
			parseTime("9999-12-31T23:59:59.55555Z"),
		},
		"6": {
			parseTime("0000-00-00T00:00:00.000000Z"),
			parseTime("1000-01-01T00:00:00.666666Z"),
			parseTime("1975-01-01T00:00:01.666666Z"),
			parseTime("1985-01-01T00:00:01.666666Z"),
			parseTime("1999-12-31T23:59:59.666666Z"),
			parseTime("2018-11-08T19:26:00.666666Z"),
			parseTime("2038-01-19T03:14:07.666666Z"),
			parseTime("9999-12-31T23:59:59.666666Z"),
		},
	}
	for length, vals := range inputs {
		t.Run(length, func(t *testing.T) {
			tbl := suite.createTable(mysql.ColumnTypeDatetime, length, attrNone)
			defer tbl.drop(t)

			for _, v := range vals {
				t.Run(v.Format(time.RFC3339Nano), func(t *testing.T) {
					suite.insertAndCompare(t, tbl, v)
				})
			}
		})
	}
}

func parseTime(str string) time.Time {
	t, _ := time.Parse(str, time.RFC3339Nano)
	return t
}
