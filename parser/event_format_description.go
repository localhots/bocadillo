package parser

import (
	"fmt"
	"strconv"
	"strings"
)

// FormatDescription is a description of binary log format.
type FormatDescription struct {
	Version                uint16
	ServerVersion          string
	CreateTimestamp        uint32
	EventHeaderLength      uint8
	EventTypeHeaderLengths []uint8
	ServerDetails          ServerDetails
}

// ServerDetails contains server feature details.
type ServerDetails struct {
	Flavor            Flavor
	Version           int
	ChecksumAlgorithm ChecksumAlgorithm
}

// Flavor defines the specific kind of MySQL-like database.
type Flavor string

// ChecksumAlgorithm is a checksum algorithm is the one used by the server.
type ChecksumAlgorithm byte

const (
	// FlavorMySQL is the MySQL db flavor.
	FlavorMySQL = "MySQL"
)

const (
	// ChecksumAlgorithmNone means no checksum appened.
	ChecksumAlgorithmNone ChecksumAlgorithm = 0x00
	// ChecksumAlgorithmCRC32 used to append a 4 byte checksum at the end.
	ChecksumAlgorithmCRC32 ChecksumAlgorithm = 0x01
	// ChecksumAlgorithmUndefined is used when checksum algorithm is not known.
	ChecksumAlgorithmUndefined ChecksumAlgorithm = 0xFF
)

// Spec: https://dev.mysql.com/doc/internals/en/format-description-event.html
func decodeFormatDescription(data []byte) FormatDescription {
	buf := newReadBuffer(data)
	fd := FormatDescription{
		Version:                buf.readUint16(),
		ServerVersion:          string(trimString(buf.readStringVarLen(50))),
		CreateTimestamp:        buf.readUint32(),
		EventHeaderLength:      buf.readUint8(),
		EventTypeHeaderLengths: buf.readStringEOF(),
	}
	fd.ServerDetails = ServerDetails{
		Flavor:            FlavorMySQL,
		Version:           parseVersionNumber(fd.ServerVersion),
		ChecksumAlgorithm: ChecksumAlgorithmUndefined,
	}
	if fd.ServerDetails.Version > 50601 {
		// Last 5 bytes are:
		// [1] Checksum algorithm
		// [4] Checksum
		fd.ServerDetails.ChecksumAlgorithm = ChecksumAlgorithm(data[len(data)-5])
		fd.EventTypeHeaderLengths = fd.EventTypeHeaderLengths[:len(fd.EventTypeHeaderLengths)-5]
	}

	return fd
}

func (fd FormatDescription) tableIDSize(et EventType) int {
	if fd.headerLen(et) == 6 {
		return 4
	}
	return 6
}

func (fd FormatDescription) headerLen(et EventType) int {
	return int(fd.EventTypeHeaderLengths[et-1])
}

func (ca ChecksumAlgorithm) String() string {
	switch ca {
	case ChecksumAlgorithmNone:
		return "None"
	case ChecksumAlgorithmCRC32:
		return "CRC32"
	case ChecksumAlgorithmUndefined:
		return "Undefined"
	default:
		return fmt.Sprintf("Unknown(%d)", ca)
	}
}

// parseVersionNumber turns string version into a number just like the library
// mysql_get_server_version function does.
// Example: 5.7.19-log gets represented as 50719
// Spec: https://dev.mysql.com/doc/refman/8.0/en/mysql-get-server-version.html
func parseVersionNumber(v string) int {
	tokens := strings.Split(v, ".")
	major, _ := strconv.Atoi(tokens[0])
	minor, _ := strconv.Atoi(tokens[1])
	var patch int
	for i, c := range tokens[2] {
		if c < '0' || c > '9' {
			patch, _ = strconv.Atoi(tokens[2][:i])
			break
		}
	}
	return major*10000 + minor*100 + patch
}
