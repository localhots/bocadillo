package blt

import (
	"encoding/binary"
	"fmt"
)

// Protocol::FixedLengthInteger
// A fixed-length integer stores its value in a series of bytes with the least
// significant byte first (little endian).
// Spec: https://dev.mysql.com/doc/internals/en/integer.html#fixed-length-integer

// int<1>

func encodeUint8(data []byte, v uint8) {
	data[0] = v
}

func decodeUint8(data []byte) uint8 {
	return uint8(data[0])
}

// int<2>

func encodeUint16(data []byte, v uint16) {
	binary.LittleEndian.PutUint16(data, v)
}

func decodeUint16(data []byte) uint16 {
	return binary.LittleEndian.Uint16(data)
}

// int<3>

func encodeUint24(data []byte, v uint32) {
	encodeVarLen64(data, uint64(v), 3)
}

func decodeUint24(data []byte) uint32 {
	return uint32(decodeVarLen64(data, 3))
}

// int<4>

func encodeUint32(data []byte, v uint32) {
	binary.LittleEndian.PutUint32(data, v)
}

func decodeUint32(data []byte) uint32 {
	return binary.LittleEndian.Uint32(data)
}

// int<6>

func encodeUint48(data []byte, v uint64) {
	encodeVarLen64(data, v, 6)
}

func decodeUint48(data []byte) uint64 {
	return decodeVarLen64(data, 6)
}

// int<8>

func encodeUint64(data []byte, v uint64) {
	binary.LittleEndian.PutUint64(data, v)
}

func decodeUint64(data []byte) uint64 {
	return binary.LittleEndian.Uint64(data)
}

// Protocol::LengthEncodedInteger
// An integer that consumes 1, 3, 4, or 9 bytes, depending on its numeric value.
// Spec: https://dev.mysql.com/doc/internals/en/integer.html#length-encoded-integer

// To convert a number value into a length-encoded integer:
// If the value is < 251, it is stored as a 1-byte integer.
// If the value is ≥ 251 and < (2^16), it is stored as 0xFC + 2-byte integer.
// If the value is ≥ (2^16) and < (2^24), it is stored as 0xFD + 3-byte integer.
// If the value is ≥ (2^24) and < (2^64) it is stored as 0xFE + 8-byte integer.
// Note: up to MySQL 3.22, 0xFE was followed by a 4-byte integer.
func encodeUintLenEnc(data []byte, v uint64, isNull bool) (size int) {
	switch {
	case isNull:
		data[0] = 0xFB
		return 1
	case v <= 0xFB:
		data[0] = byte(v)
		return 1
	case v <= 2<<15:
		data[0] = 0xFC
		encodeVarLen64(data[1:], v, 2)
		return 3
	case v <= 2<<23:
		data[0] = 0xFD
		encodeVarLen64(data[1:], v, 3)
		return 4
	default:
		data[0] = 0xFE
		encodeVarLen64(data[1:], v, 8)
		return 9
	}
}

// To convert a length-encoded integer into its numeric value, check the first
// byte:
// If it is < 0xFB, treat it as a 1-byte integer.
// If it is 0xFC, it is followed by a 2-byte integer.
// If it is 0xFD, it is followed by a 3-byte integer.
// If it is 0xFE, it is followed by a 8-byte integer.
// Depending on the context, the first byte may also have other meanings:
// If it is 0xFB, it is represents a NULL in a ProtocolText::ResultsetRow.
// If it is 0xFF and is the first byte of an ERR_Packet
// Caution:
// If the first byte of a packet is a length-encoded integer and its byte value
// is 0xFE, you must check the length of the packet to verify that it has enough
// space for a 8-byte integer.
// If not, it may be an EOF_Packet instead.
func decodeUintLenEnc(data []byte) (v uint64, isNull bool, size int) {
	switch data[0] {
	case 0xFB:
		return 0xFB, true, 1
	case 0xFC:
		return decodeVarLen64(data[1:], 2), false, 3
	case 0xFD:
		return decodeVarLen64(data[1:], 3), false, 4
	case 0xFE:
		return decodeVarLen64(data[1:], 8), false, 9
	default:
		return uint64(data[0]), false, 1
	}
}

//
// Variable length encoding helpers
//

func encodeVarLen64(data []byte, v uint64, s int) {
	for i := 0; i < s; i++ {
		data[i] = byte(v >> uint(i*8))
	}
}

func decodeVarLen64(data []byte, s int) uint64 {
	v := uint64(data[0])
	for i := 1; i < s; i++ {
		v |= uint64(data[i]) << uint(i*8)
	}
	return v
}

// Protocol::NulTerminatedString
// Strings that are terminated by a 0x00 byte.
func decodeStringNullTerm(data []byte) []byte {
	for i, c := range data {
		if c == 0x00 {
			s := make([]byte, i+1)
			copy(s, data[:i])
			return s
		}
	}

	s := make([]byte, len(data))
	copy(s, data)
	return s
}

// Protocol::VariableLengthString
// The length of the string is determined by another field or is calculated at
// runtime.
// Protocol::FixedLengthString
// Fixed-length strings have a known, hardcoded length.
func encodeStringVarLen(data, str []byte) {
	copy(data, str)
}

func decodeStringVarLen(data []byte, n int) []byte {
	return decodeStringEOF(data[:n])
}

// Protocol::LengthEncodedString
// A length encoded string is a string that is prefixed with length encoded
// integer describing the length of the string.
// It is a special case of Protocol::VariableLengthString
func decodeStringLenEnc(data []byte) (str []byte, size int) {
	strlen, _, size := decodeUintLenEnc(data)
	strleni := int(strlen)
	s := make([]byte, strleni)
	copy(s, data[size:size+strleni])
	return s, size + strleni
}

// Protocol::RestOfPacketString
// If a string is the last component of a packet, its length can be calculated
// from the overall packet length minus the current position.
func decodeStringEOF(data []byte) []byte {
	s := make([]byte, len(data))
	copy(s, data)
	return s
}

func trimString(str []byte) string {
	fmt.Println(str, string(str))
	for i, c := range str {
		if c == 0x00 {
			return string(str[:i])
		}
	}
	return string(str)
}
