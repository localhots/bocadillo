package tests

import (
	"database/sql"
	"fmt"
	"log"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/localhots/bocadillo/mysql"
	"github.com/localhots/bocadillo/reader"
)

type testSuite struct {
	reader *reader.Reader
	conn   *sql.DB
}

const (
	attrNone byte = 1 << iota
	attrUnsigned
	attrBinary
	attrAllowNull
)

type table struct {
	name   string
	colTyp mysql.ColumnType
	attrs  byte
	conn   *sql.DB
}

//
// Table operations
//

func (s *testSuite) createTable(typ mysql.ColumnType, length string, attrs byte) *table {
	name := strings.ToLower(typ.String()) + fmt.Sprintf("_test_%d", time.Now().UnixNano())
	cols := colDef(typ, length, attrs)
	_, err := s.conn.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS %s`, name))
	if err != nil {
		log.Fatal(err)
	}

	tableQuery := fmt.Sprintf(`CREATE TABLE %s (
	%s
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`, name, cols)

	fmt.Println("--------")
	fmt.Printf("-- Creating test table: type %s\n", typ.String())
	fmt.Println(tableQuery)
	fmt.Println("--------")

	_, err = s.conn.Exec(tableQuery)
	if err != nil {
		log.Fatal(err)
	}
	return &table{
		name:   name,
		colTyp: typ,
		attrs:  attrs,
		conn:   s.conn,
	}
}

func (tbl *table) insert(t *testing.T, val interface{}) {
	t.Helper()
	if val == nil {
		val = "NULL"
	}
	// log.Printf("Table: %s Value: %v", tbl.name, val)
	_, err := tbl.conn.Exec(fmt.Sprintf(`INSERT INTO %s VALUES (?)`, tbl.name), val)
	if err != nil {
		t.Fatal(err)
	}
}

func (tbl *table) drop(t *testing.T) {
	_, err := tbl.conn.Exec("DROP TABLE " + tbl.name)
	if err != nil {
		t.Fatal(err)
	}
	t.Helper()
}

func colDef(ct mysql.ColumnType, length string, attrs byte) string {
	if length != "" {
		length = "(" + length + ")"
	}

	attrWords := []string{}
	if attrUnsigned&attrs > 0 {
		attrWords = append(attrWords, "UNSIGNED")
	}
	if attrAllowNull&attrs > 0 {
		attrWords = append(attrWords, "NULL")
	} else {
		attrWords = append(attrWords, "NOT NULL")
	}

	colName := strings.ToLower(ct.String()) + "_col"
	typName, extraAttrs := colTypeSyntax(ct)
	if extraAttrs != "" {
		attrWords = append([]string{extraAttrs}, attrWords...)
	}

	return fmt.Sprintf("%s %s%s %s", colName, typName, length, strings.Join(attrWords, " "))
}

func colTypeSyntax(ct mysql.ColumnType) (typName, attrs string) {
	switch ct {
	case mysql.ColumnTypeTiny:
		return "TINYINT", ""
	case mysql.ColumnTypeShort:
		return "SMALLINT", ""
	case mysql.ColumnTypeInt24:
		return "MEDIUMINT", ""
	case mysql.ColumnTypeLong:
		return "INT", ""
	case mysql.ColumnTypeLonglong:
		return "BIGINT", ""
	case mysql.ColumnTypeFloat:
		return "FLOAT", ""
	case mysql.ColumnTypeDouble:
		return "DOUBLE", ""
	case mysql.ColumnTypeDecimal:
		return "DECIMAL", ""

	case mysql.ColumnTypeYear:
		return "YEAR", ""
	case mysql.ColumnTypeDate:
		return "DATE", ""
	case mysql.ColumnTypeTime, mysql.ColumnTypeTime2:
		return "TIME", ""
	case mysql.ColumnTypeTimestamp, mysql.ColumnTypeTimestamp2:
		return "TIMESTAMP", ""
	case mysql.ColumnTypeDatetime, mysql.ColumnTypeDatetime2:
		return "DATETIME", ""

	case mysql.ColumnTypeString:
		return "CHAR", "CHARACTER SET utf8mb4"
	case mysql.ColumnTypeVarchar:
		return "VARCHAR", "CHARACTER SET utf8mb4"
	default:
		panic(fmt.Errorf("Syntax not defined for %s", ct.String()))
	}
}

//
// Expectations
//

func (s *testSuite) expectValue(t *testing.T, tbl *table, exp interface{}) {
	t.Helper()
	out := make(chan interface{})
	go func() {
		for {
			evt, err := suite.reader.ReadEvent()
			if err != nil {
				t.Fatalf("Failed to read event: %v", err)
			}
			if evt.Table != nil && evt.Table.TableName == tbl.name {
				// pretty.Println(evt)
				out <- evt.Rows.Rows[0][0]
				return
			}
		}
	}()

	select {
	case res := <-out:
		s.compare(t, tbl, exp, res)
	case <-time.After(3 * time.Second):
		t.Fatalf("Value was not received")
	}
}

func (s *testSuite) compare(t *testing.T, tbl *table, exp, res interface{}) {
	// Sign integer if necessary
	if attrUnsigned&tbl.attrs == 0 {
		// old := res
		res = signNumber(res, tbl.colTyp)
		// t.Logf("Converted unsigned %d into signed %d", old, res)
	}

	// Expectations would be pointers for null types, dereference them because
	// they will be compared to values
	if reflect.TypeOf(exp).Kind() == reflect.Ptr {
		expv := reflect.ValueOf(exp)
		if !expv.IsNil() {
			exp = expv.Elem().Interface()
		} else {
			exp = nil
		}
	}

	// fmt.Printf("VALUE RECEIVED: %T(%+v), EXPECTED: %T(%+v)\n", res, res, exp, exp)
	if exp != res {
		t.Errorf("Expected %T(%+v), got %T(%+v)", exp, exp, res, res)
	}
}

func (s *testSuite) insertAndCompare(t *testing.T, tbl *table, val interface{}) {
	t.Helper()
	tbl.insert(t, val)
	suite.expectValue(t, tbl, val)
}

func signNumber(val interface{}, ct mysql.ColumnType) interface{} {
	switch tval := val.(type) {
	case uint8:
		return mysql.SignUint8(tval)
	case *uint8:
		if tval == nil {
			return nil
		}
		return mysql.SignUint8(*tval)
	case uint16:
		return mysql.SignUint16(tval)
	case uint32:
		if ct == mysql.ColumnTypeInt24 {
			return mysql.SignUint24(tval)
		}
		return mysql.SignUint32(tval)
	case uint64:
		return mysql.SignUint64(tval)
	default:
		return val
	}
}
