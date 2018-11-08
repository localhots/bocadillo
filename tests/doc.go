// Package tests contains integration tests. Test suite maintains two
// connections to the database: one for a client that operates tables and
// inserts new records into them, and another that reads binary log and looks
// for entries created by the test. Value received by the client and the value
// read from the binary log are then compared.
package tests
