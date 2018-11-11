package binlog

import "github.com/localhots/bocadillo/mysql"

// XIDEvent contains an XID (XA transaction identifier)
// https://dev.mysql.com/doc/refman/5.7/en/xa.html
type XIDEvent struct {
	XID uint64
}

// Decode decodes given buffer into an XID event.
// Spec: https://dev.mysql.com/doc/internals/en/xid-event.html
func (e *XIDEvent) Decode(connBuff []byte) {
	e.XID = mysql.DecodeUint64(connBuff)
}
