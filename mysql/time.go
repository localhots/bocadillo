package mysql

import (
	"encoding/binary"
	"fmt"
	"time"
)

// Timezone is set for decoded datetime values.
var Timezone = time.UTC

// DecodeYear decodes YEAR value.
// Spec: https://dev.mysql.com/doc/refman/8.0/en/year.html
func DecodeYear(v uint8) uint16 {
	return uint16(v) + 1900
}

// DecodeDate decodes DATE value.
// Spec: https://dev.mysql.com/doc/refman/8.0/en/datetime.html
func DecodeDate(v uint32) string {
	if v == 0 {
		return "0000-00-00"
	}
	return fmt.Sprintf("%04d-%02d-%02d", v/(16*32), v/32%16, v%32)
}

// DecodeTime decodes TIME value.
// Spec: https://dev.mysql.com/doc/refman/8.0/en/time.html
func DecodeTime(v uint32) string {
	if v == 0 {
		return "00:00:00"
	}
	var sign string
	if v < 0 {
		sign = "-"
	}
	return fmt.Sprintf("%s%02d:%02d:%02d", sign, v/10000, (v%10000)/100, v%100)
}

// DecodeTime2 decodes TIME v2 value.
// Implementation borrowed from https://github.com/siddontang/go-mysql/
func DecodeTime2(data []byte, dec uint16) (string, int) {
	const offset int64 = 0x800000000000
	const intOffset int64 = 0x800000
	// time  binary length
	n := int(3 + (dec+1)/2)

	tmp := int64(0)
	intPart := int64(0)
	frac := int64(0)
	switch dec {
	case 1:
	case 2:
		intPart = int64(DecodeVarLen64BigEndian(data[0:3])) - intOffset
		frac = int64(data[3])
		if intPart < 0 && frac > 0 {
			intPart++     // Shift to the next integer value
			frac -= 0x100 // -(0x100 - frac)
		}
		tmp = intPart<<24 + frac*10000
	case 3:
	case 4:
		intPart = int64(DecodeVarLen64BigEndian(data[0:3])) - intOffset
		frac = int64(binary.BigEndian.Uint16(data[3:5]))
		if intPart < 0 && frac > 0 {
			// Fix reverse fractional part order: "0x10000 - frac".
			// See comments for FSP=1 and FSP=2 above.
			intPart++       // Shift to the next integer value
			frac -= 0x10000 // -(0x10000-frac)
		}
		tmp = intPart<<24 + frac*100

	case 5:
	case 6:
		tmp = int64(DecodeVarLen64BigEndian(data[0:6])) - offset
	default:
		intPart = int64(DecodeVarLen64BigEndian(data[0:3])) - intOffset
		tmp = intPart << 24
	}

	if intPart == 0 {
		return "00:00:00", n
	}

	hms := int64(0)
	sign := ""
	if tmp < 0 {
		tmp = -tmp
		sign = "-"
	}

	hms = tmp >> 24

	hour := (hms >> 12) % (1 << 10) // 10 bits starting at 12th
	minute := (hms >> 6) % (1 << 6) // 6 bits starting at 6th
	second := hms % (1 << 6)        // 6 bits starting at 0th
	secPart := tmp % (1 << 24)

	if secPart != 0 {
		return fmt.Sprintf("%s%02d:%02d:%02d.%06d", sign, hour, minute, second, secPart), n
	}

	return fmt.Sprintf("%s%02d:%02d:%02d", sign, hour, minute, second), n
}

// DecodeTimestamp decodes TIMESTAMP value.
// Spec: https://dev.mysql.com/doc/refman/8.0/en/datetime.html
// Implementation borrowed from https://github.com/siddontang/go-mysql/
func DecodeTimestamp(data []byte, dec uint16) (time.Time, int) {
	return time.Unix(int64(DecodeUint32(data)), 0), 4
}

// DecodeTimestamp2 decodes TIMESTAMP v2 value.
// Spec: https://dev.mysql.com/doc/refman/8.0/en/datetime.html
// Implementation borrowed from https://github.com/siddontang/go-mysql/
func DecodeTimestamp2(data []byte, dec uint16) (time.Time, int) {
	// get timestamp binary length
	n := int(4 + (dec+1)/2)
	sec := int64(binary.BigEndian.Uint32(data[0:4]))
	usec := int64(0)
	switch dec {
	case 1, 2:
		usec = int64(data[4]) * 10000
	case 3, 4:
		usec = int64(binary.BigEndian.Uint16(data[4:])) * 100
	case 5, 6:
		usec = int64(DecodeVarLen64BigEndian(data[4:7]))
	}

	if sec == 0 {
		return time.Time{}, n
	}

	return time.Unix(sec, usec*1000), n
}

// DecodeDatetime decodes DATETIME value.
// Spec: https://dev.mysql.com/doc/refman/8.0/en/datetime.html
func DecodeDatetime(v uint64) time.Time {
	d := v / 1000000
	t := v % 1000000
	return time.Date(int(d/10000),
		time.Month((d%10000)/100),
		int(d%100),
		int(t/10000),
		int((t%10000)/100),
		int(t%100),
		0,
		Timezone,
	)
}

// DecodeDatetime2 decodes DATETIME v2 value.
// Spec: https://dev.mysql.com/doc/refman/8.0/en/datetime.html
// Implementation borrowed from https://github.com/siddontang/go-mysql/
func DecodeDatetime2(data []byte, dec uint16) (time.Time, int) {
	const offset int64 = 0x8000000000
	// get datetime binary length
	n := int(5 + (dec+1)/2)

	intPart := int64(DecodeVarLen64BigEndian(data[0:5])) - offset
	var frac int64

	switch dec {
	case 1, 2:
		frac = int64(data[5]) * 10000
	case 3, 4:
		frac = int64(binary.BigEndian.Uint16(data[5:7])) * 100
	case 5, 6:
		frac = int64(DecodeVarLen64BigEndian(data[5:8]))
	}

	if intPart == 0 {
		return time.Time{}, n
	}

	tmp := intPart<<24 + frac
	// handle sign???
	if tmp < 0 {
		tmp = -tmp
	}

	// var secPart int64 = tmp % (1 << 24)
	ymdhms := tmp >> 24

	ymd := ymdhms >> 17
	ym := ymd >> 5
	hms := ymdhms % (1 << 17)

	day := int(ymd % (1 << 5))
	month := int(ym % 13)
	year := int(ym / 13)

	second := int(hms % (1 << 6))
	minute := int((hms >> 6) % (1 << 6))
	hour := int((hms >> 12))

	return time.Date(year, time.Month(month), day, hour, minute, second, int(frac*1000), Timezone), n
}
