package parser

func (r *Reader) decodeRotateEvent(data []byte) Position {
	buf := newReadBuffer(data)
	var p Position
	if r.format.Version > 1 {
		p.Offset = buf.readUint64()
	} else {
		p.Offset = 4
	}
	p.File = string(buf.readStringEOF())
	return p
}
