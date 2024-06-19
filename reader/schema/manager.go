package schema

import (
	"database/sql"
	"regexp"
	"strings"
)

// Manager maintains table schemas.
type Manager struct {
	Schema *Schema
	db     *sql.DB
}

// NewManager creates a new schema manager.
func NewManager(db *sql.DB) *Manager {
	return &Manager{
		Schema: NewSchema(),
		db:     db,
	}
}

// Manage adds given tables to a list of managed tables and updates its details.
func (m *Manager) Manage(database, table string) error {
	cols, err := m.tableColumns(database, table)
	if err != nil {
		return err
	}

	m.Schema.Update(database, table, cols)
	return nil
}

// ProcessQuery accepts an SQL query and updates schema if required.
func (m *Manager) ProcessQuery(database, query string) error {
	if tableName, ok := changedTable(query); ok {
		if tbl := m.Schema.Table(database, tableName); tbl != nil {
			return m.Manage(database, tableName)
		}
	}
	return nil
}

func (m *Manager) tableColumns(database, table string) ([]Column, error) {
	rows, err := m.db.Query(`
		SELECT COLUMN_NAME, COLUMN_TYPE 
		FROM INFORMATION_SCHEMA.COLUMNS 
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? 
		ORDER BY ORDINAL_POSITION ASC
	`, database, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols := make([]Column, 0)
	for rows.Next() {
		var col Column
		var typ string
		err := rows.Scan(&col.Name, &typ)
		if err != nil {
			return nil, err
		}
		if strings.Contains(strings.ToLower(typ), "unsigned") {
			col.Unsigned = true
		}
		cols = append(cols, col)
	}
	return cols, nil
}

var alterRegexp = regexp.MustCompile(`(?im)^alter[\s\t\n]+table[\s\t\n]+` + "`" + `?([a-z0-9_]+)`)

func changedTable(query string) (string, bool) {
	m := alterRegexp.FindAllStringSubmatch(query, -1)
	if len(m) > 0 {
		return m[0][1], true
	}
	return "", false
}
