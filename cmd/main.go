package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/localhots/blt/parser"
	"github.com/localhots/gobelt/log"
)

func main() {
	dsn := flag.String("dsn", "", "Database source name")
	id := flag.Uint("id", 1000, "Server ID (arbitrary, unique)")
	file := flag.String("file", "", "Binary log file name")
	offset := flag.Uint("offset", 0, "Log offset in bytes")
	flag.Parse()

	ctx := context.Background()
	validate((*dsn != ""), "Database source name is not set")
	validate((*id != 0), "Server ID is not set")
	validate((*file != ""), "Binary log file is not set")
	conf := parser.Config{
		ServerID: uint32(*id),
		File:     *file,
		Offset:   uint32(*offset),
	}

	reader, err := parser.Connect(*dsn, conf)
	if err != nil {
		log.Fatalf(ctx, "Failed to establish connection: %v", err)
	}

	off := conf.Offset
	for i := 0; i < 100; i++ {
		// for {
		evt, err := reader.ReadEventHeader(ctx)
		if err != nil {
			log.Fatalf(ctx, "Failed to read event: %v", err)
		}
		ts := time.Unix(int64(evt.Timestamp), 0).Format(time.RFC3339)
		log.Info(ctx, "Event received", log.F{
			"type":      evt.Type,
			"timestamp": ts,
			"offset":    off,
		})
		off = evt.NextOffset
	}
}

func validate(cond bool, msg string) {
	if !cond {
		fmt.Println(msg)
		flag.Usage()
		os.Exit(2)
	}
}
