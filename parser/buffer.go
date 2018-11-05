package parser

import (
	"encoding/binary"
)

// buffer is a simple wrapper over a slice of bytes with a cursor. It allows for
// easy command building and results parsing.
type buffer struct {
	data []byte
	pos  int
}

// skip next n bytes
func (b *buffer) skip(n int) {
	b.pos += n
}

// advance skips next N bytes and returns them
func (b *buffer) advance(n int) []byte {
	b.skip(n)
	return b.data[b.pos-n:]
}

// cur returns remaining unread buffer.
func (b *buffer) cur() []byte {
	return b.data[b.pos:]
}

// newReadBuffer creates a buffer with command output.
func newReadBuffer(data []byte) *buffer {
	return &buffer{data: data}
}

func (b *buffer) readUint8() uint8 {
	return decodeUint8(b.advance(1))
}

func (b *buffer) readUint16() uint16 {
	return decodeUint16(b.advance(2))
}

func (b *buffer) readUint24() uint32 {
	return decodeUint24(b.advance(3))
}

func (b *buffer) readUint32() uint32 {
	return decodeUint32(b.advance(4))
}

func (b *buffer) readUint48() uint64 {
	return decodeUint48(b.advance(6))
}

func (b *buffer) readUint64() uint64 {
	return decodeUint64(b.advance(8))
}

func (b *buffer) readUintLenEnc() (val uint64, isNull bool) {
	var size int
	val, isNull, size = decodeUintLenEnc(b.cur())
	b.skip(size)
	return
}

func (b *buffer) readStringNullTerm() []byte {
	str := decodeStringNullTerm(b.cur())
	b.skip(len(str) + 1)
	return str
}

func (b *buffer) readStringVarLen(n int) []byte {
	return decodeStringVarLen(b.advance(n), n)
}

func (b *buffer) readStringLenEnc() []byte {
	str, size := decodeStringLenEnc(b.cur())
	b.skip(size)
	return str
}

func (b *buffer) readStringEOF() []byte {
	return decodeStringEOF(b.cur())
}

// Pre-allocate command buffer. First four bytes would be used to set command
// length and sequence number.
func newCommandBuffer(size int) *buffer {
	return &buffer{data: make([]byte, size+4), pos: 4}
}

func (b *buffer) writeByte(v byte) {
	b.data[b.pos] = v
	b.pos++
}

func (b *buffer) writeUint16(v uint16) {
	binary.LittleEndian.PutUint16(b.data[b.pos:], v)
	b.pos += 2
}

func (b *buffer) writeUint32(v uint32) {
	binary.LittleEndian.PutUint32(b.data[b.pos:], v)
	b.pos += 4
}

func (b *buffer) writeString(s string) {
	b.data[b.pos] = byte(len(s))
	b.pos++
	b.pos += copy(b.data[b.pos:], s)
}

func (b *buffer) writeStringEOF(s string) {
	b.pos += copy(b.data[b.pos:], s)
}
