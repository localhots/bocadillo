package schema

// Schema contains table definitions.
type Schema struct {
	tables map[string]map[string]Table
}

// Table is a list of columns.
type Table struct {
	columns []Column
}

// Column carries two key column parameters that are not available in the binary
// log of older versions of MySQL.
type Column struct {
	Name string
	// Unsigned is true if the column is of integer or decimal types and is
	// unsigned.
	Unsigned bool
}

// NewSchema creates a new managed schema object.
func NewSchema() *Schema {
	return &Schema{tables: make(map[string]map[string]Table)}
}

// Table returns table details for a given database and table name pair. If the
// table can't be found nil is returned.
func (s Schema) Table(database, table string) *Table {
	if d, ok := s.tables[database]; ok {
		if t, ok := d[table]; ok {
			return &t
		}
	}
	return nil
}

// Update sets new column definitions for a given database and table name pair.
func (s Schema) Update(database, table string, cols []Column) {
	if _, ok := s.tables[database]; !ok {
		s.tables[database] = make(map[string]Table)
	}
	s.tables[database][table] = Table{columns: cols}
}

// Column returns column details for the given column index. If index is out of
// range nil is returned.
func (t Table) Column(i int) *Column {
	if i >= 0 && i < len(t.columns) {
		return &t.columns[i]
	}
	return nil
}
