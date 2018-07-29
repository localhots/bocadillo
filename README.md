# BLT

MySQL binary log parser.

### WIP

Work in progress, some events are not fully supported.

A fork of [go-sql-driver/mysql](https://github.com/go-sql-driver/mysql) is used
to provide underlying connection and basic packet exchange.

**TODO:**

* [x] FormatDescriptionEvent
* [x] TableMapEvent
* [x] RotateEvent
* [ ] RowsEvent
* [ ] XIDEvent
* [ ] GTIDEvent
* [ ] QueryEvent

### Licence

MIT
