package mysql

import (
	"unsafe"
)

// SignUint8 converts uint8 into int8.
func SignUint8(v uint8) int8 {
	return *(*int8)(unsafe.Pointer(&v))
}

// SignUint16 converts uint16 into int16.
func SignUint16(v uint16) int16 {
	return *(*int16)(unsafe.Pointer(&v))
}

// SignUint24 converts 3-byte uint32 into int32.
func SignUint24(v uint32) int32 {
	if v&0x00800000 != 0 {
		v |= 0xFF000000
	}
	return *(*int32)(unsafe.Pointer(&v))
}

// SignUint32 converts uint32 into int32.
func SignUint32(v uint32) int32 {
	return *(*int32)(unsafe.Pointer(&v))
}

// SignUint64 converts uint64 into int64.
func SignUint64(v uint64) int64 {
	return *(*int64)(unsafe.Pointer(&v))
}
