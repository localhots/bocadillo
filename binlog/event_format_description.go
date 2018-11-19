package binlog

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/localhots/bocadillo/tools"
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

// FormatDescriptionEvent contains server details and binary log format
// description. It is usually the first event in a log file.
type FormatDescriptionEvent struct {
	FormatDescription
}

// Flavor defines the specific kind of MySQL-like database.
type Flavor string

// ChecksumAlgorithm is a checksum algorithm is the one used by the server.
type ChecksumAlgorithm byte

const (
	// FlavorMySQL is the MySQL db flavor.
	FlavorMySQL = "MySQL"

	// ChecksumAlgorithmNone means no checksum appened.
	ChecksumAlgorithmNone ChecksumAlgorithm = 0x00
	// ChecksumAlgorithmCRC32 used to append a 4 byte checksum at the end.
	ChecksumAlgorithmCRC32 ChecksumAlgorithm = 0x01
	// ChecksumAlgorithmUndefined is used when checksum algorithm is not known.
	ChecksumAlgorithmUndefined ChecksumAlgorithm = 0xFF
)

// Decode decodes given buffer into a format description event.
// Spec: https://dev.mysql.com/doc/internals/en/format-description-event.html
func (e *FormatDescriptionEvent) Decode(data []byte) error {
	buf := tools.NewBuffer(data)
	e.Version = buf.ReadUint16()
	e.ServerVersion = trimStringEOF(buf.ReadStringVarLen(50))
	e.CreateTimestamp = buf.ReadUint32()
	e.EventHeaderLength = buf.ReadUint8()
	e.EventTypeHeaderLengths = buf.ReadStringEOF()
	e.ServerDetails = ServerDetails{
		Flavor:            FlavorMySQL,
		Version:           parseVersionNumber(e.ServerVersion),
		ChecksumAlgorithm: ChecksumAlgorithmUndefined,
	}
	if e.ServerDetails.Version > 50601 {
		e.ServerDetails.ChecksumAlgorithm = ChecksumAlgorithm(data[len(data)-5])
		e.EventTypeHeaderLengths = e.EventTypeHeaderLengths[:len(e.EventTypeHeaderLengths)-5]
	}

	return nil
}

// HeaderLen returns length of event header.
func (fd FormatDescription) HeaderLen() int {
	const defaultHeaderLength = 19
	if fd.EventHeaderLength > 0 {
		return int(fd.EventHeaderLength)
	}
	return defaultHeaderLength
}

// PostHeaderLen returns length of a post-header for a given event type.
func (fd FormatDescription) PostHeaderLen(et EventType) int {
	return int(fd.EventTypeHeaderLengths[et-1])
}

// TableIDSize returns table ID size for a given event type.
func (fd FormatDescription) TableIDSize(et EventType) int {
	if fd.PostHeaderLen(et) == 6 {
		return 4
	}
	return 6
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

func trimStringEOF(str []byte) string {
	for i, c := range str {
		if c == 0x00 {
			return string(str[:i])
		}
	}
	return string(str)
}
