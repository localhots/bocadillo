package schema

import (
	"database/sql"
	"strings"
)

// SchemaManager maintains table schemas.
type SchemaManager struct {
	Schema *Schema
	db     *sql.DB
}

// NewManager creates a new schema manager.
func NewManager(db *sql.DB) *SchemaManager {
	return &SchemaManager{
		Schema: NewSchema(),
		db:     db,
	}
}

// Manage adds given tables to a list of managed tables and updates its details.
func (m *SchemaManager) Manage(database, table string) error {
	cols, err := m.tableColumns(database, table)
	if err != nil {
		return err
	}

	m.Schema.Update(database, table, cols)
	return nil
}

// ProcessQuery accepts an SQL query and updates schema if required.
func (m *SchemaManager) ProcessQuery(query string) error {
	if strings.HasPrefix(query, "ALTER TABLE") {
		for database, tables := range m.Schema.tables {
			for table := range tables {
				if err := m.Manage(database, table); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (m *SchemaManager) tableColumns(database, table string) ([]Column, error) {
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
