package messages

import "litebase/server/sqlite3"

type QueryMessageResponse struct {
	Changes         int64
	Columns         []string
	ID              []byte
	Latency         float64
	LastInsertRowID int64
	RowCount        int
	Rows            [][]*sqlite3.Column
}
