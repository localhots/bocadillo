package mysql

import (
	"encoding/json"
	"fmt"
	"math"

	"github.com/juju/errors"
)

const (
	jsonSmallObject byte = iota
	jsonLargeObject
	jsonSmallArray
	jsonLargeArray
	jsonLiteral // Literal (true/false/null)
	jsonInt16
	jsonUint16
	jsonInt32
	jsonUint32
	jsonInt64
	jsonUint64
	jsonFloat64
	jsonString
	jsonOpaque byte = 0x0f // Custom data (any MySQL data type)

	jsonNull  byte = 0x00
	jsonTrue  byte = 0x01
	jsonFalse byte = 0x02

	jsonbSmallOffsetSize = 2
	jsonbLargeOffsetSize = 4

	jsonbKeyEntrySizeSmall = 2 + jsonbSmallOffsetSize
	jsonbKeyEntrySizeLarge = 2 + jsonbLargeOffsetSize

	jsonbValueEntrySizeSmall = 1 + jsonbSmallOffsetSize
	jsonbValueEntrySizeLarge = 1 + jsonbLargeOffsetSize
)

// DecodeJSON decodes JSON into raw bytes.
// Implementation borrowed from https://github.com/siddontang/go-mysql/
func DecodeJSON(data []byte) ([]byte, error) {
	d := jsonBinaryDecoder{useDecimal: false}

	if d.isDataShort(data, 1) {
		return nil, d.err
	}

	v := d.decodeValue(data[0], data[1:])
	if d.err != nil {
		return nil, d.err
	}

	return json.Marshal(v)
}

func jsonbGetOffsetSize(isSmall bool) int {
	if isSmall {
		return jsonbSmallOffsetSize
	}

	return jsonbLargeOffsetSize
}

func jsonbGetKeyEntrySize(isSmall bool) int {
	if isSmall {
		return jsonbKeyEntrySizeSmall
	}

	return jsonbKeyEntrySizeLarge
}

func jsonbGetValueEntrySize(isSmall bool) int {
	if isSmall {
		return jsonbValueEntrySizeSmall
	}

	return jsonbValueEntrySizeLarge
}

type jsonBinaryDecoder struct {
	useDecimal bool
	err        error
}

func (d *jsonBinaryDecoder) decodeValue(tp byte, data []byte) interface{} {
	if d.err != nil {
		return nil
	}

	switch tp {
	case jsonSmallObject:
		return d.decodeObjectOrArray(data, true, true)
	case jsonLargeObject:
		return d.decodeObjectOrArray(data, false, true)
	case jsonSmallArray:
		return d.decodeObjectOrArray(data, true, false)
	case jsonLargeArray:
		return d.decodeObjectOrArray(data, false, false)
	case jsonLiteral:
		return d.decodeLiteral(data)
	case jsonInt16:
		return d.decodeInt16(data)
	case jsonUint16:
		return d.decodeUint16(data)
	case jsonInt32:
		return d.decodeInt32(data)
	case jsonUint32:
		return d.decodeUint32(data)
	case jsonInt64:
		return d.decodeInt64(data)
	case jsonUint64:
		return d.decodeUint64(data)
	case jsonFloat64:
		return d.decodeDouble(data)
	case jsonString:
		return d.decodeString(data)
	case jsonOpaque:
		return d.decodeOpaque(data)
	default:
		d.err = errors.Errorf("invalid json type %d", tp)
	}

	return nil
}

func (d *jsonBinaryDecoder) decodeObjectOrArray(data []byte, isSmall bool, isObject bool) interface{} {
	offsetSize := jsonbGetOffsetSize(isSmall)
	if d.isDataShort(data, 2*offsetSize) {
		return nil
	}

	count := d.decodeCount(data, isSmall)
	size := d.decodeCount(data[offsetSize:], isSmall)

	if d.isDataShort(data, int(size)) {
		return nil
	}

	keyEntrySize := jsonbGetKeyEntrySize(isSmall)
	valueEntrySize := jsonbGetValueEntrySize(isSmall)

	headerSize := 2*offsetSize + count*valueEntrySize

	if isObject {
		headerSize += count * keyEntrySize
	}

	if headerSize > size {
		d.err = errors.Errorf("header size %d > size %d", headerSize, size)
		return nil
	}

	var keys []string
	if isObject {
		keys = make([]string, count)
		for i := 0; i < count; i++ {
			// decode key
			entryOffset := 2*offsetSize + keyEntrySize*i
			keyOffset := d.decodeCount(data[entryOffset:], isSmall)
			keyLength := int(d.decodeUint16(data[entryOffset+offsetSize:]))

			// Key must start after value entry
			if keyOffset < headerSize {
				d.err = errors.Errorf("invalid key offset %d, must > %d", keyOffset, headerSize)
				return nil
			}

			if d.isDataShort(data, keyOffset+keyLength) {
				return nil
			}

			keys[i] = string(data[keyOffset : keyOffset+keyLength])
		}
	}

	if d.err != nil {
		return nil
	}

	values := make([]interface{}, count)
	for i := 0; i < count; i++ {
		// decode value
		entryOffset := 2*offsetSize + valueEntrySize*i
		if isObject {
			entryOffset += keyEntrySize * count
		}

		tp := data[entryOffset]

		if isInlineValue(tp, isSmall) {
			values[i] = d.decodeValue(tp, data[entryOffset+1:entryOffset+valueEntrySize])
			continue
		}

		valueOffset := d.decodeCount(data[entryOffset+1:], isSmall)

		if d.isDataShort(data, valueOffset) {
			return nil
		}

		values[i] = d.decodeValue(tp, data[valueOffset:])
	}

	if d.err != nil {
		return nil
	}

	if !isObject {
		return values
	}

	m := make(map[string]interface{}, count)
	for i := 0; i < count; i++ {
		m[keys[i]] = values[i]
	}

	return m
}

func isInlineValue(tp byte, isSmall bool) bool {
	switch tp {
	case jsonInt16, jsonUint16, jsonLiteral:
		return true
	case jsonInt32, jsonUint32:
		return !isSmall
	}

	return false
}

func (d *jsonBinaryDecoder) decodeLiteral(data []byte) interface{} {
	if d.isDataShort(data, 1) {
		return nil
	}

	tp := data[0]

	switch tp {
	case jsonNull:
		return nil
	case jsonTrue:
		return true
	case jsonFalse:
		return false
	}

	d.err = errors.Errorf("invalid literal %c", tp)

	return nil
}

func (d *jsonBinaryDecoder) isDataShort(data []byte, expected int) bool {
	if d.err != nil {
		return true
	}

	if len(data) < expected {
		d.err = errors.Errorf("data len %d < expected %d", len(data), expected)
	}

	return d.err != nil
}

func (d *jsonBinaryDecoder) decodeInt16(data []byte) int16 {
	if d.isDataShort(data, 2) {
		return 0
	}

	v := SignUint16(DecodeUint16(data))
	return v
}

func (d *jsonBinaryDecoder) decodeUint16(data []byte) uint16 {
	if d.isDataShort(data, 2) {
		return 0
	}

	v := DecodeUint16(data)
	return v
}

func (d *jsonBinaryDecoder) decodeInt32(data []byte) int32 {
	if d.isDataShort(data, 4) {
		return 0
	}

	v := SignUint32(DecodeUint32(data))
	return v
}

func (d *jsonBinaryDecoder) decodeUint32(data []byte) uint32 {
	if d.isDataShort(data, 4) {
		return 0
	}

	v := DecodeUint32(data)
	return v
}

func (d *jsonBinaryDecoder) decodeInt64(data []byte) int64 {
	if d.isDataShort(data, 8) {
		return 0
	}

	v := SignUint64(DecodeUint64(data))
	return v
}

func (d *jsonBinaryDecoder) decodeUint64(data []byte) uint64 {
	if d.isDataShort(data, 8) {
		return 0
	}

	v := DecodeUint64(data)
	return v
}

func (d *jsonBinaryDecoder) decodeDouble(data []byte) float64 {
	if d.isDataShort(data, 8) {
		return 0
	}

	v := DecodeFloat64(data)
	return v
}

func (d *jsonBinaryDecoder) decodeString(data []byte) string {
	if d.err != nil {
		return ""
	}

	l, n := d.decodeVariableLength(data)

	if d.isDataShort(data, l+n) {
		return ""
	}

	data = data[n:]

	v := string(data[0:l])
	return v
}

func (d *jsonBinaryDecoder) decodeOpaque(data []byte) interface{} {
	if d.isDataShort(data, 1) {
		return nil
	}

	tp := data[0]
	data = data[1:]

	l, n := d.decodeVariableLength(data)

	if d.isDataShort(data, l+n) {
		return nil
	}

	data = data[n : l+n]

	switch ColumnType(tp) {
	case ColumnTypeNewDecimal:
		return d.decodeDecimal(data)
	case ColumnTypeTime:
		return d.decodeTime(data)
	case ColumnTypeDate,
		ColumnTypeDatetime, ColumnTypeDatetime2,
		ColumnTypeTimestamp, ColumnTypeTimestamp2:
		return d.decodeDateTime(data)
	default:
		return string(data)
	}
}

func (d *jsonBinaryDecoder) decodeDecimal(data []byte) interface{} {
	precision := int(data[0])
	scale := int(data[1])

	v, _ := DecodeDecimal(data[2:], precision, scale)

	return v
}

func (d *jsonBinaryDecoder) decodeTime(data []byte) interface{} {
	v := d.decodeInt64(data)

	if v == 0 {
		return "00:00:00"
	}

	sign := ""
	if v < 0 {
		sign = "-"
		v = -v
	}

	intPart := v >> 24
	hour := (intPart >> 12) % (1 << 10)
	min := (intPart >> 6) % (1 << 6)
	sec := intPart % (1 << 6)
	frac := v % (1 << 24)

	return fmt.Sprintf("%s%02d:%02d:%02d.%06d", sign, hour, min, sec, frac)
}

func (d *jsonBinaryDecoder) decodeDateTime(data []byte) interface{} {
	v := d.decodeInt64(data)
	if v == 0 {
		return "0000-00-00 00:00:00"
	}

	// handle negative?
	if v < 0 {
		v = -v
	}

	intPart := v >> 24
	ymd := intPart >> 17
	ym := ymd >> 5
	hms := intPart % (1 << 17)

	year := ym / 13
	month := ym % 13
	day := ymd % (1 << 5)
	hour := (hms >> 12)
	minute := (hms >> 6) % (1 << 6)
	second := hms % (1 << 6)
	frac := v % (1 << 24)

	return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d.%06d", year, month, day, hour, minute, second, frac)

}

func (d *jsonBinaryDecoder) decodeCount(data []byte, isSmall bool) int {
	if isSmall {
		v := d.decodeUint16(data)
		return int(v)
	}

	return int(d.decodeUint32(data))
}

func (d *jsonBinaryDecoder) decodeVariableLength(data []byte) (int, int) {
	// The max size for variable length is math.MaxUint32, so
	// here we can use 5 bytes to save it.
	maxCount := 5
	if len(data) < maxCount {
		maxCount = len(data)
	}

	pos := 0
	length := uint64(0)
	for ; pos < maxCount; pos++ {
		v := data[pos]
		length |= uint64(v&0x7F) << uint(7*pos)

		if v&0x80 == 0 {
			if length > math.MaxUint32 {
				d.err = errors.Errorf("variable length %d must <= %d", length, math.MaxUint32)
				return 0, 0
			}

			pos++
			// TODO: should consider length overflow int here.
			return int(length), pos
		}
	}

	d.err = errors.New("decode variable length failed")

	return 0, 0
}
