package blt

import (
	"fmt"
)

// EventType defines a binary log event type.
type EventType byte

// Spec: https://dev.mysql.com/doc/internals/en/event-classes-and-types.html
const (
	// UnknownEvent is an event that should never occur.
	UnknownEvent EventType = 0
	// StartEventV3 is the Start_event of binlog format 3.
	StartEventV3 EventType = 1
	// QueryEvent is created for each query that modifies the database, unless
	// the query is logged row-based.
	QueryEvent EventType = 2
	// StopEvent is written to the log files under these circumstances:
	// A master writes the event to the binary log when it shuts down.
	// A slave writes the event to the relay log when it shuts down or when a
	// RESET SLAVE statement is executed.
	StopEvent EventType = 3
	// RotateEvent is written at the end of the file that points to the next
	// file in the squence. It is written when a binary log file exceeds a size
	// limit.
	RotateEvent EventType = 4
	// IntvarEvent will be created just before a Query_event, if the query uses
	// one of the variables LAST_INSERT_ID or INSERT_ID.
	IntvarEvent EventType = 5
	// LoadEvent ...
	LoadEvent EventType = 6
	// SlaveEvent ...
	SlaveEvent EventType = 7
	// CreateFileEvent ...
	CreateFileEvent EventType = 8
	// AppendBlockEvent is created to contain the file data.
	AppendBlockEvent EventType = 9
	// ExecLoadEvent ...
	ExecLoadEvent EventType = 10
	// DeleteFileEvent occurs when the LOAD DATA failed on the master.
	// This event notifies the slave not to do the load and to delete the
	// temporary file.
	DeleteFileEvent EventType = 11
	// NewLoadEvent ...
	NewLoadEvent EventType = 12
	// RandEvent logs random seed used by the next RAND(), and by PASSWORD()
	// in 4.1.0.
	RandEvent EventType = 13
	// UserVarEvent is written every time a statement uses a user variable;
	// precedes other events for the statement. Indicates the value to use for
	// the user variable in the next statement. This is written only before a
	// QUERY_EVENT and is not used with row-based logging.
	UserVarEvent EventType = 14
	// FormatDescriptionEvent is saved by threads which read it, as they need it
	// for future use (to decode the ordinary events).
	FormatDescriptionEvent EventType = 15
	// XIDEvent is generated for a commit of a transaction that modifies one or
	// more tables of an XA-capable storage engine.
	XIDEvent EventType = 16
	// BeginLoadQueryEvent is for the first block of file to be loaded, its only
	// difference from Append_block event is that this event creates or
	// truncates existing file before writing data.
	BeginLoadQueryEvent EventType = 17
	// ExecuteLoadQueryEvent is responsible for LOAD DATA execution, it similar
	// to Query_event but before executing the query it substitutes original
	// filename in LOAD DATA query with name of temporary file.
	ExecuteLoadQueryEvent EventType = 18
	// TableMapEvent is used in row-based mode where it preceeds every row
	// operation event and maps a table definition to a number. The table
	// definition consists of database name, table name, and column definitions.
	TableMapEvent EventType = 19
	// WriteRowsEventV0 represents inserted rows. Used in MySQL 5.1.0 to 5.1.15.
	WriteRowsEventV0 EventType = 20
	// UpdateRowsEventV0 represents updated rows. It contains both old and new
	// versions. Used in MySQL 5.1.0 to 5.1.15.
	UpdateRowsEventV0 EventType = 21
	// DeleteRowsEventV0 represents deleted rows. Used in MySQL 5.1.0 to 5.1.15.
	DeleteRowsEventV0 EventType = 22
	// WriteRowsEventV1 represents inserted rows. Used in MySQL 5.1.15 to 5.6.
	WriteRowsEventV1 EventType = 23
	// UpdateRowsEventV1 represents updated rows. It contains both old and new
	// versions. Used in MySQL 5.1.15 to 5.6.
	UpdateRowsEventV1 EventType = 24
	// DeleteRowsEventV1 represents deleted rows. Used in MySQL 5.1.15 to 5.6.
	DeleteRowsEventV1 EventType = 25
	// IncidentEvent represents an incident, an occurance out of the ordinary,
	// that happened on the master. The event is used to inform the slave that
	// something out of the ordinary happened on the master that might cause the
	// database to be in an inconsistent state.
	IncidentEvent EventType = 26
	// HeartbeetEvent is a replication event used to ensure to slave that master
	// is alive. The event is originated by master's dump thread and sent
	// straight to slave without being logged. Slave itself does not store it in
	// relay log but rather uses a data for immediate checks and throws away the
	// event.
	HeartbeetEvent EventType = 27
	// IgnorableEvent is a kind of event that could be ignored.
	IgnorableEvent EventType = 28
	// RowsQueryEvent is a subclass of the IgnorableEvent, to record the
	// original query for the rows events in RBR.
	RowsQueryEvent EventType = 29
	// WriteRowsEventV2 represents inserted rows. Used starting from MySQL 5.6.
	WriteRowsEventV2 EventType = 30
	// UpdateRowsEventV2 represents updated rows. It contains both old and new
	// versions. Used starting from MySQL 5.6.
	UpdateRowsEventV2 EventType = 31
	// DeleteRowsEventV2 represents deleted rows. Used starting from MySQL 5.6.
	DeleteRowsEventV2 EventType = 32
	// GTIDEvent is an event that contains latest GTID.
	// GTID stands for Global Transaction IDentifier It is composed of two
	// parts:
	// * SID for Source Identifier, and
	// * GNO for Group Number. The basic idea is to associate an identifier, the
	// Global Transaction IDentifier or GTID, to every transaction. When a
	// transaction is copied to a slave, re-executed on the slave, and written
	// to the slave's binary log, the GTID is preserved. When a slave connects
	// to a master, the slave uses GTIDs instead of (file, offset).
	GTIDEvent EventType = 33
	// AnonymousGTIDEvent is a subclass of GTIDEvent.
	AnonymousGTIDEvent EventType = 34
	// PreviousGTIDsEvent is a subclass of GTIDEvent.
	PreviousGTIDsEvent EventType = 35
)

func (et EventType) isEither(types ...EventType) bool {
	for _, t := range types {
		if et == t {
			return true
		}
	}
	return false
}

func (et EventType) String() string {
	switch et {
	case UnknownEvent:
		return "UnknownEvent"
	case StartEventV3:
		return "StartEventV3"
	case QueryEvent:
		return "QueryEvent"
	case StopEvent:
		return "StopEvent"
	case RotateEvent:
		return "RotateEvent"
	case IntvarEvent:
		return "IntvarEvent"
	case LoadEvent:
		return "LoadEvent"
	case SlaveEvent:
		return "SlaveEvent"
	case CreateFileEvent:
		return "CreateFileEvent"
	case AppendBlockEvent:
		return "AppendBlockEvent"
	case ExecLoadEvent:
		return "ExecLoadEvent"
	case DeleteFileEvent:
		return "DeleteFileEvent"
	case NewLoadEvent:
		return "NewLoadEvent"
	case RandEvent:
		return "RandEvent"
	case UserVarEvent:
		return "UserVarEvent"
	case FormatDescriptionEvent:
		return "FormatDescriptionEvent"
	case XIDEvent:
		return "XIDEvent"
	case BeginLoadQueryEvent:
		return "BeginLoadQueryEvent"
	case ExecuteLoadQueryEvent:
		return "ExecuteLoadQueryEvent"
	case TableMapEvent:
		return "TableMapEvent"
	case WriteRowsEventV0:
		return "WriteRowsEventV0"
	case UpdateRowsEventV0:
		return "UpdateRowsEventV0"
	case DeleteRowsEventV0:
		return "DeleteRowsEventV0"
	case WriteRowsEventV1:
		return "WriteRowsEventV1"
	case UpdateRowsEventV1:
		return "UpdateRowsEventV1"
	case DeleteRowsEventV1:
		return "DeleteRowsEventV1"
	case IncidentEvent:
		return "IncidentEvent"
	case HeartbeetEvent:
		return "HeartbeetEvent"
	case IgnorableEvent:
		return "IgnorableEvent"
	case RowsQueryEvent:
		return "RowsQueryEvent"
	case WriteRowsEventV2:
		return "WriteRowsEventV2"
	case UpdateRowsEventV2:
		return "UpdateRowsEventV2"
	case DeleteRowsEventV2:
		return "DeleteRowsEventV2"
	case GTIDEvent:
		return "GTIDEvent"
	case AnonymousGTIDEvent:
		return "AnonymousGTIDEvent"
	case PreviousGTIDsEvent:
		return "PreviousGTIDsEvent"
	default:
		return fmt.Sprintf("Unknown(%d)", et)
	}
}
