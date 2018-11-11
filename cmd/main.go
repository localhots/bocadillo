package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/localhots/bocadillo/reader"
	"github.com/localhots/bocadillo/reader/slave"
)

func main() {
	dsn := flag.String("dsn", "", "Database source name")
	id := flag.Uint("id", 1000, "Server ID (arbitrary, unique)")
	file := flag.String("file", "", "Binary log file name")
	offset := flag.Uint("offset", 0, "Log offset in bytes")
	flag.Parse()

	validate((*dsn != ""), "Database source name is not set")
	validate((*id != 0), "Server ID is not set")
	validate((*file != ""), "Binary log file is not set")

	reader, err := reader.New(*dsn, slave.Config{
		ServerID: uint32(*id),
		File:     *file,
		Offset:   uint32(*offset),
	})
	if err != nil {
		log.Fatalf("Failed to create reader: %v", err)
	}

	// for i := 0; i < 100; i++ {
	for {
		evt, err := reader.ReadEvent()
		if err != nil {
			log.Fatalf("Failed to read event: %v", err)
		}
		ts := time.Unix(int64(evt.Header.Timestamp), 0).Format(time.RFC3339)
		log.Printf("Event received: %s %s, %d\n", evt.Header.Type.String(), ts, evt.Header.NextOffset)

		if evt.Table != nil {
			_, err := evt.DecodeRows()
			if err != nil {
				log.Fatalf("Failed to parse rows event: %v", err)
			}
		}
	}
}

func validate(cond bool, msg string) {
	if !cond {
		fmt.Println(msg)
		flag.Usage()
		os.Exit(2)
	}
}
