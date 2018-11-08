package tools

import (
	"encoding/binary"

	"github.com/localhots/bocadillo/mysql"
)

// Buffer is a simple wrapper over a slice of bytes with a cursor. It allows for
// easy command building and results parsing.
type Buffer struct {
	data []byte
	pos  int
}

// NewBuffer creates a new buffer from a given slice of bytes and sets the
// cursor to the beginning.
func NewBuffer(data []byte) *Buffer {
	return &Buffer{data: data}
}

// NewCommandBuffer pre-allocates a buffer of a given size and reserves 4 bytes
// at the beginning for the driver, these would be used to set command length
// and sequence number.
func NewCommandBuffer(size int) *Buffer {
	return &Buffer{data: make([]byte, size+4), pos: 4}
}

// Skip advances the cursor by N bytes.
func (b *Buffer) Skip(n int) {
	b.pos += n
}

// Read returns next N bytes and advances the cursor.
func (b *Buffer) Read(n int) []byte {
	b.pos += n
	return b.data[b.pos-n:]
}

// Cur returns remaining unread buffer.
func (b *Buffer) Cur() []byte {
	return b.data[b.pos:]
}

// More returns true if there's more to read.
func (b *Buffer) More() bool {
	return b.pos < len(b.data)
}

// Bytes returns entire buffer contents.
func (b *Buffer) Bytes() []byte {
	return b.data
}

// ReadUint8 reads a uint8 and advances cursor by 1 byte.
func (b *Buffer) ReadUint8() uint8 {
	return mysql.DecodeUint8(b.Read(1))
}

// ReadUint16 reads a uint16 and advances cursor by 2 bytes.
func (b *Buffer) ReadUint16() uint16 {
	return mysql.DecodeUint16(b.Read(2))
}

// ReadUint24 reads a 3-byte integer as uint32 and advances cursor by 3 bytes.
func (b *Buffer) ReadUint24() uint32 {
	return mysql.DecodeUint24(b.Read(3))
}

// ReadUint32 reads a uint32 and advances cursor by 4 bytes.
func (b *Buffer) ReadUint32() uint32 {
	return mysql.DecodeUint32(b.Read(4))
}

// ReadUint48 reads a 6-byte integer as uint64 and advances cursor by 6 bytes.
func (b *Buffer) ReadUint48() uint64 {
	return mysql.DecodeUint48(b.Read(6))
}

// ReadUint64 reads a uint64 and advances cursor by 8 bytes.
func (b *Buffer) ReadUint64() uint64 {
	return mysql.DecodeUint64(b.Read(8))
}

// ReadUintLenEnc reads a length-encoded integer and advances cursor accordingly.
func (b *Buffer) ReadUintLenEnc() (val uint64, isNull bool, size int) {
	val, isNull, size = mysql.DecodeUintLenEnc(b.Cur())
	b.Skip(size)
	return
}

// ReadVarLen64 reads a number encoded in given size of bytes and advances
// cursor accordingly.
func (b *Buffer) ReadVarLen64(n int) uint64 {
	return mysql.DecodeVarLen64(b.Read(n), n)
}

// ReadFloat32 reads a float32 and advances cursor by 4 bytes.
func (b *Buffer) ReadFloat32() float32 {
	return mysql.DecodeFloat32(b.Read(4))
}

// ReadFloat64 reads a float64 and advances cursor by 8 bytes.
func (b *Buffer) ReadFloat64() float64 {
	return mysql.DecodeFloat64(b.Read(8))
}

// ReadStringNullTerm reads a NULL-terminated string and advances cursor by its
// length plus 1 extra byte.
func (b *Buffer) ReadStringNullTerm() []byte {
	str := mysql.DecodeStringNullTerm(b.Cur())
	b.Skip(len(str) + 1)
	return str
}

// ReadStringVarLen reads a variable-length string and advances cursor by the
// same number of bytes.
func (b *Buffer) ReadStringVarLen(n int) []byte {
	return mysql.DecodeStringVarLen(b.Read(n), n)
}

// ReadStringVarEnc reads a variable-length length of the string and the string
// itself, then advances cursor by the same number of bytes.
func (b *Buffer) ReadStringVarEnc(n int) []byte {
	length := int(mysql.DecodeVarLen64(b.Read(n), n))
	return mysql.DecodeStringVarLen(b.Read(length), length)
}

// ReadStringLenEnc reads a length-encoded string and advances cursor
// accordingly.
func (b *Buffer) ReadStringLenEnc() (str []byte, size int) {
	str, size = mysql.DecodeStringLenEnc(b.Cur())
	b.Skip(size)
	return
}

// ReadStringEOF reads remaining contents of the buffer as a new string.
func (b *Buffer) ReadStringEOF() []byte {
	return mysql.DecodeStringEOF(b.Cur())
}

// WriteByte writes given byte to the buffer and advances cursor by 1.
func (b *Buffer) WriteByte(v byte) {
	b.data[b.pos] = v
	b.pos++
}

// WriteUint16 writes given uint16 value to the buffer and advances cursor by 2.
func (b *Buffer) WriteUint16(v uint16) {
	binary.LittleEndian.PutUint16(b.data[b.pos:], v)
	b.pos += 2
}

// WriteUint32 writes given uint32 value to the buffer and advances cursor by 4.
func (b *Buffer) WriteUint32(v uint32) {
	binary.LittleEndian.PutUint32(b.data[b.pos:], v)
	b.pos += 4
}

// WriteStringLenEnc writes a length-encoded string to the buffer and advances
// cursor accordingly.
func (b *Buffer) WriteStringLenEnc(s string) {
	b.data[b.pos] = byte(len(s))
	b.pos++
	b.pos += copy(b.data[b.pos:], s)
}

// WriteStringEOF writes given string to the buffer and advances cursor by its
// length.
func (b *Buffer) WriteStringEOF(s string) {
	b.pos += copy(b.data[b.pos:], s)
}
