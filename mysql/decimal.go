package mysql

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// DecodeDecimal decodes a decimal value.
// Implementation borrowed from https://github.com/siddontang/go-mysql/
func DecodeDecimal(data []byte, precision int, decimals int) (string, int) {
	const digitsPerInteger int = 9
	var compressedBytes = [...]int{0, 1, 1, 2, 2, 3, 3, 4, 4, 4}

	decodeDecimalDecompressValue := func(compIndx int, data []byte, mask uint8) (size int, value uint32) {
		size = compressedBytes[compIndx]
		databuff := make([]byte, size)
		for i := 0; i < size; i++ {
			databuff[i] = data[i] ^ mask
		}
		value = uint32(DecodeVarLen64BigEndian(databuff))
		return
	}
	// See python mysql replication and https://github.com/jeremycole/mysql_binlog
	integral := (precision - decimals)
	uncompIntegral := int(integral / digitsPerInteger)
	uncompFractional := int(decimals / digitsPerInteger)
	compIntegral := integral - (uncompIntegral * digitsPerInteger)
	compFractional := decimals - (uncompFractional * digitsPerInteger)

	binSize := uncompIntegral*4 + compressedBytes[compIntegral] +
		uncompFractional*4 + compressedBytes[compFractional]

	buf := make([]byte, binSize)
	copy(buf, data[:binSize])

	// Must copy the data for later change
	data = buf

	// Support negative
	// The sign is encoded in the high bit of the the byte
	// But this bit can also be used in the value
	value := uint32(data[0])
	var res bytes.Buffer
	var mask uint32
	if value&0x80 == 0 {
		mask = uint32((1 << 32) - 1)
		res.WriteString("-")
	}

	// Clear sign
	data[0] ^= 0x80

	pos, value := decodeDecimalDecompressValue(compIntegral, data, uint8(mask))
	res.WriteString(fmt.Sprintf("%d", value))

	for i := 0; i < uncompIntegral; i++ {
		value = binary.BigEndian.Uint32(data[pos:]) ^ mask
		pos += 4
		res.WriteString(fmt.Sprintf("%09d", value))
	}

	res.WriteString(".")

	for i := 0; i < uncompFractional; i++ {
		value = binary.BigEndian.Uint32(data[pos:]) ^ mask
		pos += 4
		res.WriteString(fmt.Sprintf("%09d", value))
	}

	if size, value := decodeDecimalDecompressValue(compFractional, data[pos:], uint8(mask)); size > 0 {
		res.WriteString(fmt.Sprintf("%0*d", compFractional, value))
		pos += size
	}

	return res.String(), pos
}
