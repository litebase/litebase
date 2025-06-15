package messages

import "github.com/litebase/litebase/server/sqlite3"

type QueryMessageResponse struct {
	Changes         int64
	Columns         []string
	Error           string
	ID              []byte
	Latency         float64
	LastInsertRowID int64
	RowCount        int
	Rows            [][]*sqlite3.Column
	TransactionID   []byte
	WALSequence     int64
	WALTimestamp    int64
}
