package messages

import "github.com/litebase/litebase/pkg/sqlite3"

type QueryMessageResponse struct {
	Changes         int64
	Columns         []string
	Error           string
	ID              string
	Latency         float64
	LastInsertRowID int64
	RowCount        int
	Rows            [][]*sqlite3.Column
	TransactionID   string
	WALSequence     int64
	WALTimestamp    int64
}
