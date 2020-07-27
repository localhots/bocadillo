# Bocadillo

Bocadillo is a client for MySQL binary log. It is not a complete solution (yet).

### Usage

Example use:
```go
// import "github.com/localhots/bocadillo/reader"
// import "github.com/localhots/bocadillo/reader/driver"

reader, err := reader.New("root@(127.0.0.1:3306)/testdb", driver.Config{
	ServerID: 1000,               // Arbitrary unique ID
	File:     "mysql-bin.000035", // Log file name
	Offset:   4,                  // Log file offset
})
if err != nil {
	log.Fatalf("Failed to connect: %v", err)
}

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

### Caveats

This library is not a complete solution. It requires implementation that would
involve everything from configuration to state management. Future releases
might include pre-made binaries for certain message queue adapters.

### Future development & contributions

The package in its current state does the job for me. Bug reports are welcome
just like feature contributions.

### Go MySQL driver modifications

Modified copy of [go-sql-driver/mysql](https://github.com/go-sql-driver/mysql)
is included with this project. It was changed in order to expose certain low
level functions that allow to establish a connection manually and register as a
replica server and to remove automatic driver registration because it will
likely conflict with the original code when imported as a dependency.

### Licence

Mozilla Public License Version 2.0

This project includes a modified copy of [go-sql-driver/mysql](https://github.com/go-sql-driver/mysql)
which is licensed under MPL-2.0, hence it should be licensed under the same
lincense (or a GPL one).
