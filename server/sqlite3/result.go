package sqlite3

type Result struct {
	Changes int64
	Columns []string
	Rows    [][]Column
}

func (r *Result) ColumnNames() []string {
	return r.Columns
}

// Next
func (r *Result) Next() []Column {
	if len(r.Rows) == 0 {
		return nil
	}

	row := r.Rows[0]
	r.Rows = r.Rows[1:]

	return row
}
