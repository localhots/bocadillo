package schema

import "testing"

func TestChangedTable(t *testing.T) {
	inputs := []struct {
		query, tbl string
		ok         bool
	}{
		{"alter\ttable foobar add column", "foobar", true},
		{"ALTER   TABLE foobar ADD COLUMN", "foobar", true},
		{"alter table    `foobar` add column", "foobar", true},
		{"ALTER TABLE `foobar` ADD COLUMN", "foobar", true},
		{"alter\ntable     \n\tfoobar\nadd column", "foobar", true},
		{"ALTER TABLE Foo_Bar111 ADD COLUMN", "Foo_Bar111", true},
		{"SELECT * FROM foobar", "", false},
		{"SELECT * FROM `foobar`", "", false},
	}

	for _, in := range inputs {
		out, ok := changedTable(in.query)
		if ok != in.ok {
			if ok {
				t.Errorf("Didn't expect to match table %q in query %q", out, in.query)
			} else {
				t.Errorf("Expected to match table %q in query %q", in.tbl, in.query)
			}
			continue
		}
		if out != in.tbl {
			t.Errorf("Expected to match table %q, got %q in query %q", in.tbl, out, in.query)
		}
	}
}
