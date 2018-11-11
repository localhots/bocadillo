package tools

import (
	"fmt"

	"github.com/localhots/pretty"
)

// EnableDebug controls debug output.
var EnableDebug = false

// Debug ...
func Debug(vals ...interface{}) {
	if EnableDebug {
		pretty.Println(vals...)
	}
}

// Debugf ...
func Debugf(format string, args ...interface{}) {
	if EnableDebug {
		fmt.Printf(format, args...)
	}
}
