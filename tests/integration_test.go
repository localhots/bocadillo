package tests

import (
	"database/sql"
	"log"
	"os"
	"strconv"
	"testing"

	_ "github.com/localhots/mysql"

	"github.com/localhots/bocadillo/binlog"
	"github.com/localhots/bocadillo/reader"
)

var suite *testSuite

func TestMain(m *testing.M) {
	dsn, conf := getConfig()

	conn, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatal(err)
	}

	if conf.File == "" {
		pos := getLatestOffset(conn)
		log.Printf("File is not set, using latest from master: %s @ %d", pos.File, pos.Offset)
		conf.File = pos.File
		conf.Offset = uint32(pos.Offset)
	}

	slaveConn, err := reader.Connect(dsn, conf)
	if err != nil {
		log.Fatal(err)
	}

	rdr, err := reader.NewReader(slaveConn)
	if err != nil {
		log.Fatal(err)
	}

	suite = &testSuite{
		reader: rdr,
		conn:   conn,
	}

	exitCode := m.Run()

	os.Exit(exitCode)
}

func getConfig() (dsn string, conf reader.Config) {
	envOrDefault := func(name, def string) string {
		if val := os.Getenv(name); val != "" {
			return val
		}
		return def
	}
	makeUint32 := func(str string) uint32 {
		u64, err := strconv.ParseUint(str, 10, 32)
		if err != nil {
			log.Fatal(err)
		}
		return uint32(u64)
	}

	dsn = envOrDefault("TEST_DSN", "root@(127.0.0.1:3306)/loltest")
	conf.ServerID = makeUint32(envOrDefault("TEST_SERVER_ID", "1000"))
	conf.File = envOrDefault("TEST_FILE", "")
	conf.Offset = makeUint32(envOrDefault("TEST_OFFSET", "4"))
	hostname, _ := os.Hostname()
	conf.Hostname = envOrDefault("TEST_HOSTNAME", hostname)

	return
}

func getLatestOffset(conn *sql.DB) binlog.Position {
	var pos binlog.Position
	var _discard interface{}
	err := conn.QueryRow("SHOW MASTER STATUS").Scan(
		&pos.File, &pos.Offset, &_discard, &_discard, &_discard,
	)
	if err != nil {
		log.Fatal(err)
	}
	return pos
}