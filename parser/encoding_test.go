package parser

import "testing"

func TestEncodeUint8(t *testing.T) {
	buf := make([]byte, 1)
	encodeUint8(buf, 123)
	t.Log(buf)
}
