package sqlite3

type Result struct {
	Changes int64
	Columns []string
	Rows    [][]Column
}
