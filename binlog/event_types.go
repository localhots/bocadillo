package binlog

import (
	"fmt"
)

// EventType defines a binary log event type.
type EventType byte

// Spec: https://dev.mysql.com/doc/internals/en/event-classes-and-types.html
const (
	// EventTypeUnknown is an event that should never occur.
	EventTypeUnknown EventType = 0
	// EventTypeStartV3 is the Start_event of binlog format 3.
	EventTypeStartV3 EventType = 1
	// EventTypeQuery is created for each query that modifies the database,
	// unless the query is logged row-based.
	EventTypeQuery EventType = 2
	// EventTypeStop is written to the log files under these circumstances:
	// A master writes the event to the binary log when it shuts down.
	// A slave writes the event to the relay log when it shuts down or when a
	// RESET SLAVE statement is executed.
	EventTypeStop EventType = 3
	// EventTypeRotate is written at the end of the file that points to the next
	// file in the squence. It is written when a binary log file exceeds a size
	// limit.
	EventTypeRotate EventType = 4
	// EventTypeIntvar will be created just before a Query_event, if the query
	// uses one of the variables LAST_INSERT_ID or INSERT_ID.
	EventTypeIntvar EventType = 5
	// EventTypeLoad ...
	EventTypeLoad EventType = 6
	// EventTypeSlave ...
	EventTypeSlave EventType = 7
	// EventTypeCreateFile ...
	EventTypeCreateFile EventType = 8
	// EventTypeAppendBlock is created to contain the file data.
	EventTypeAppendBlock EventType = 9
	// EventTypeExecLoad ...
	EventTypeExecLoad EventType = 10
	// EventTypeDeleteFile occurs when the LOAD DATA failed on the master.
	// This event notifies the slave not to do the load and to delete the
	// temporary file.
	EventTypeDeleteFile EventType = 11
	// EventTypeNewLoad ...
	EventTypeNewLoad EventType = 12
	// EventTypeRand logs random seed used by the next RAND(), and by PASSWORD()
	// in 4.1.0.
	EventTypeRand EventType = 13
	// EventTypeUserVar is written every time a statement uses a user variable;
	// precedes other events for the statement. Indicates the value to use for
	// the user variable in the next statement. This is written only before a
	// QUERY_EVENT and is not used with row-based logging.
	EventTypeUserVar EventType = 14
	// EventTypeFormatDescription is saved by threads which read it, as they
	// need it for future use (to decode the ordinary events).
	EventTypeFormatDescription EventType = 15
	// EventTypeXID is generated for a commit of a transaction that modifies one
	// or more tables of an XA-capable storage engine.
	EventTypeXID EventType = 16
	// EventTypeBeginLoadQuery is for the first block of file to be loaded, its
	// only difference from Append_block event is that this event creates or
	// truncates existing file before writing data.
	EventTypeBeginLoadQuery EventType = 17
	// EventTypeExecuteLoadQuery is responsible for LOAD DATA execution, it
	// similar to Query_event but before executing the query it substitutes
	// original filename in LOAD DATA query with name of temporary file.
	EventTypeExecuteLoadQuery EventType = 18
	// EventTypeTableMap is used in row-based mode where it preceeds every row
	// operation event and maps a table definition to a number. The table
	// definition consists of database name, table name, and column definitions.
	EventTypeTableMap EventType = 19
	// EventTypeWriteRowsV0 represents inserted rows. Used in MySQL 5.1.0 to
	// 5.1.15.
	EventTypeWriteRowsV0 EventType = 20
	// EventTypeUpdateRowsV0 represents updated rows. It contains both old and
	// new versions. Used in MySQL 5.1.0 to 5.1.15.
	EventTypeUpdateRowsV0 EventType = 21
	// EventTypeDeleteRowsV0 represents deleted rows. Used in MySQL 5.1.0 to
	// 5.1.15.
	EventTypeDeleteRowsV0 EventType = 22
	// EventTypeWriteRowsV1 represents inserted rows. Used in MySQL 5.1.15 to
	// 5.6.
	EventTypeWriteRowsV1 EventType = 23
	// EventTypeUpdateRowsV1 represents updated rows. It contains both old and
	// new versions. Used in MySQL 5.1.15 to 5.6.
	EventTypeUpdateRowsV1 EventType = 24
	// EventTypeDeleteRowsV1 represents deleted rows. Used in MySQL 5.1.15 to
	// 5.6.
	EventTypeDeleteRowsV1 EventType = 25
	// EventTypeIncident represents an incident, an occurance out of the
	// ordinary, that happened on the master. The event is used to inform the
	// slave that something out of the ordinary happened on the master that
	// might cause the database to be in an inconsistent state.
	EventTypeIncident EventType = 26
	// EventTypeHeartbeet is a replication event used to ensure to slave that
	// master is alive. The event is originated by master's dump thread and sent
	// straight to slave without being logged. Slave itself does not store it in
	// relay log but rather uses a data for immediate checks and throws away the
	// event.
	EventTypeHeartbeet EventType = 27
	// EventTypeIgnorable is a kind of event that could be ignored.
	EventTypeIgnorable EventType = 28
	// EventTypeRowsQuery is a subclass of the IgnorableEvent, to record the
	// original query for the rows events in RBR.
	EventTypeRowsQuery EventType = 29
	// EventTypeWriteRowsV2 represents inserted rows. Used starting from MySQL
	// 5.6.
	EventTypeWriteRowsV2 EventType = 30
	// EventTypeUpdateRowsV2 represents updated rows. It contains both old and
	// new versions. Used starting from MySQL 5.6.
	EventTypeUpdateRowsV2 EventType = 31
	// EventTypeDeleteRowsV2 represents deleted rows. Used starting from MySQL
	// 5.6.
	EventTypeDeleteRowsV2 EventType = 32
	// EventTypeGTID is an event that contains latest GTID.
	// GTID stands for Global Transaction IDentifier It is composed of two
	// parts:
	// * SID for Source Identifier, and
	// * GNO for Group Number. The basic idea is to associate an identifier, the
	// Global Transaction IDentifier or GTID, to every transaction. When a
	// transaction is copied to a slave, re-executed on the slave, and written
	// to the slave's binary log, the GTID is preserved. When a slave connects
	// to a master, the slave uses GTIDs instead of (file, offset).
	EventTypeGTID EventType = 33
	// EventTypeAnonymousGTID is a subclass of GTIDEvent.
	EventTypeAnonymousGTID EventType = 34
	// EventTypePreviousGTIDs is a subclass of GTIDEvent.
	EventTypePreviousGTIDs EventType = 35
)

func (et EventType) String() string {
	switch et {
	case EventTypeUnknown:
		return "UnknownEvent"
	case EventTypeStartV3:
		return "StartEventV3"
	case EventTypeQuery:
		return "QueryEvent"
	case EventTypeStop:
		return "StopEvent"
	case EventTypeRotate:
		return "RotateEvent"
	case EventTypeIntvar:
		return "IntvarEvent"
	case EventTypeLoad:
		return "LoadEvent"
	case EventTypeSlave:
		return "SlaveEvent"
	case EventTypeCreateFile:
		return "CreateFileEvent"
	case EventTypeAppendBlock:
		return "AppendBlockEvent"
	case EventTypeExecLoad:
		return "ExecLoadEvent"
	case EventTypeDeleteFile:
		return "DeleteFileEvent"
	case EventTypeNewLoad:
		return "NewLoadEvent"
	case EventTypeRand:
		return "RandEvent"
	case EventTypeUserVar:
		return "UserVarEvent"
	case EventTypeFormatDescription:
		return "FormatDescriptionEvent"
	case EventTypeXID:
		return "XIDEvent"
	case EventTypeBeginLoadQuery:
		return "BeginLoadQueryEvent"
	case EventTypeExecuteLoadQuery:
		return "ExecuteLoadQueryEvent"
	case EventTypeTableMap:
		return "TableMapEvent"
	case EventTypeWriteRowsV0:
		return "WriteRowsEventV0"
	case EventTypeUpdateRowsV0:
		return "UpdateRowsEventV0"
	case EventTypeDeleteRowsV0:
		return "DeleteRowsEventV0"
	case EventTypeWriteRowsV1:
		return "WriteRowsEventV1"
	case EventTypeUpdateRowsV1:
		return "UpdateRowsEventV1"
	case EventTypeDeleteRowsV1:
		return "DeleteRowsEventV1"
	case EventTypeIncident:
		return "IncidentEvent"
	case EventTypeHeartbeet:
		return "HeartbeetEvent"
	case EventTypeIgnorable:
		return "IgnorableEvent"
	case EventTypeRowsQuery:
		return "RowsQueryEvent"
	case EventTypeWriteRowsV2:
		return "WriteRowsEventV2"
	case EventTypeUpdateRowsV2:
		return "UpdateRowsEventV2"
	case EventTypeDeleteRowsV2:
		return "DeleteRowsEventV2"
	case EventTypeGTID:
		return "GTIDEvent"
	case EventTypeAnonymousGTID:
		return "AnonymousGTIDEvent"
	case EventTypePreviousGTIDs:
		return "PreviousGTIDsEvent"
	default:
		return fmt.Sprintf("Unknown(%d)", et)
	}
}
