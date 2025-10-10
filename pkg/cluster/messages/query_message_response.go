package messages

import "github.com/litebase/litebase/pkg/sqlite3"

type QueryMessageResponse struct {
	Changes         int64
	Columns         []sqlite3.ColumnDefinition
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
