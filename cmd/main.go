package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/juju/errors"
	"github.com/localhots/bocadillo/mysql/slave"
	"github.com/localhots/bocadillo/reader"
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

	done := handleShutdown()
	ctx := context.Background()
	for {
		ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
		defer cancel()
		select {
		case <-done:
			log.Println("Closing reader")
			err := reader.Close()
			if err != nil {
				log.Fatalf("Failed to close reader: %v", err)
			}
			return
		default:
			evt, err := reader.ReadEvent(ctx)
			if err != nil {
				if isTimeout(err) {
					log.Println("Event read timeout")
					continue
				}
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
		cancel()
	}
}

func validate(cond bool, msg string) {
	if !cond {
		fmt.Println(msg)
		flag.Usage()
		os.Exit(2)
	}
}

func handleShutdown() <-chan struct{} {
	sig := make(chan os.Signal, 1)
	done := make(chan struct{})
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sig
		log.Println("Shutdown requested")
		close(done)
	}()
	return done
}

func isTimeout(err error) bool {
	if err == nil {
		return false
	}
	err = errors.Cause(err)
	if err == context.DeadlineExceeded || err == context.Canceled {
		return true
	}
	ne, ok := err.(*net.OpError)
	return ok && ne.Timeout()
}
