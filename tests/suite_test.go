package tests

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
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
	name string
	cols []column
	conn *sql.DB
}

type column struct {
	typ    mysql.ColumnType
	length string
	attrs  byte
}

//
// Table operations
//

func (s *testSuite) createTable(typ mysql.ColumnType, length string, attrs byte) *table {
	return s.createTableMulti(column{typ, length, attrs})
}

func (s *testSuite) createTableMulti(cols ...column) *table {
	name := fmt.Sprintf("test_table_%d", time.Now().UnixNano())
	colDefs := make([]string, len(cols))
	for i, col := range cols {
		colDefs[i] = colDef(col.typ, col.length, col.attrs)
	}
	colsDefStr := strings.Join(colDefs, ",\n\t")
	_, err := s.conn.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS %s`, name))
	if err != nil {
		log.Fatal(err)
	}

	tableQuery := fmt.Sprintf(`CREATE TABLE %s (
	%s
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`, name, colsDefStr)

	fmt.Println("--------")
	fmt.Println("-- Creating test table")
	fmt.Println(tableQuery)
	fmt.Println("--------")

	_, err = s.conn.Exec(tableQuery)
	if err != nil {
		log.Fatal(err)
	}
	return &table{
		name: name,
		cols: cols,
		conn: s.conn,
	}
}

func (tbl *table) insert(t *testing.T, vals ...interface{}) {
	t.Helper()
	ph := strings.Repeat("?,", len(vals))
	ph = ph[:len(ph)-1]

	_, err := tbl.conn.Exec(fmt.Sprintf(`INSERT INTO %s VALUES (%s)`, tbl.name, ph), vals...)
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
	case mysql.ColumnTypeTinyblob:
		return "TINYBLOB", ""
	case mysql.ColumnTypeBlob:
		return "BLOB", ""
	case mysql.ColumnTypeMediumblob:
		return "MEDIUMBLOB", ""
	case mysql.ColumnTypeLongblob:
		return "LONGBLOB", ""

	case mysql.ColumnTypeSet:
		return "SET", ""
	case mysql.ColumnTypeEnum:
		return "ENUM", ""
	case mysql.ColumnTypeJSON:
		return "JSON", ""
	case mysql.ColumnTypeGeometry:
		return "GEOMETRY", ""
	case mysql.ColumnTypeBit:
		return "BIT", ""

	default:
		panic(fmt.Errorf("Syntax not defined for %s", ct.String()))
	}
}

//
// Expectations
//

func (s *testSuite) insertAndCompare(t *testing.T, tbl *table, vals ...interface{}) {
	t.Helper()
	tbl.insert(t, vals...)
	suite.expectValue(t, tbl, vals)
}

func (s *testSuite) insertAndCompareExp(t *testing.T, tbl *table, vals, exps []interface{}) {
	t.Helper()
	tbl.insert(t, vals...)
	suite.expectValue(t, tbl, exps)
}

func (s *testSuite) expectValue(t *testing.T, tbl *table, exp []interface{}) {
	t.Helper()
	out := make(chan []interface{})
	go func() {
		for {
			evt, err := suite.reader.ReadEvent()
			if err != nil {
				t.Errorf("Failed to read event: %v", err)
				return
			}
			if evt.Table != nil && evt.Table.TableName == tbl.name {
				re, err := evt.DecodeRows()
				if err != nil {
					t.Fatalf("Failed to decode rows event: %v", err)
				}
				if len(re.Rows) != 1 {
					t.Fatal("Expected 1 row")
				}

				out <- re.Rows[0]
				return
			}
		}
	}()

	select {
	case res := <-out:
		for i := range res {
			s.compare(t, tbl.cols[i], exp[i], res[i])
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("Value was not received")
	}
}

func (s *testSuite) compare(t *testing.T, col column, exp, res interface{}) {
	// Sign integer if necessary
	if attrUnsigned&col.attrs == 0 {
		res = signNumber(res, col.typ)
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

	switch texp := exp.(type) {
	case []byte:
		switch col.typ {
		case mysql.ColumnTypeJSON:
			var jExp, jRes interface{}
			if err := json.Unmarshal(texp, &jExp); err != nil {
				panic(err)
			}
			if err := json.Unmarshal(res.([]byte), &jRes); err != nil {
				panic(err)
			}
			if !cmp.Equal(jExp, jRes) {
				t.Errorf("JSON values are different: %s", cmp.Diff(jExp, jRes))
			}
		default:
			if !bytes.Equal(texp, res.([]byte)) {
				t.Errorf("Expected %T(%+v), got %T(%+v)", exp, exp, res, res)
			}
		}
	default:
		if exp != res {
			t.Errorf("Expected %T(%+v), got %T(%+v)", exp, exp, res, res)
		}
	}
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
