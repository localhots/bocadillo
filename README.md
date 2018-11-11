# Bocadillo

Bocadillo is a parser for MySQL binary log. It can connect to a server, register
as a slave and process binary log.

### Usage

```go
// import "github.com/localhots/bocadillo/reader"

reader, _ := reader.New("root@(127.0.0.1:3306)/testdb", slave.Config{
	ServerID: 1000,               // Arbitrary unique ID
	File:     "mysql-bin.000035", // Log file name
	Offset:   4,                  // Log file offset
})

for {
	evt, err := reader.ReadEvent()
	if err != nil {
		log.Fatalf("Failed to read event: %v", err)
    }
    
	log.Println("Event received:", evt.Header.Type.String())
	if evt.Table != nil {
		rows, err := evt.DecodeRows()
		if err != nil {
			log.Fatalf("Failed to parse rows event: %v", err)
        }
        log.Println("Table:", evt.Table.TableName, "Changes:", rows.Rows)
    }
}

```

### WIP

This package is a work in progress and can't be considered production ready just
yet.

A fork of [go-sql-driver/mysql](https://github.com/localhots/mysql) is used
to provide underlying connection and basic packet exchange.

### Licence

MIT
