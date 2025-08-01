package cluster

import (
	"bytes"
	"io"

	"github.com/litebase/litebase/pkg/sqlite3"
)

type NodeQueryResponse interface {
	Changes() int64
	Columns() []string
	Encode(responseBuffer, rowsBuffer, columnsBuffer *bytes.Buffer) ([]byte, error)
	Error() string
	LastInsertRowId() int64
	Latency() float64
	RowCount() int
	Rows() [][]*sqlite3.Column
	Reset()
	SetError(err string)
	SetId(id string)
	ToMap() map[string]interface{}
	ToJSON() ([]byte, error)
	TransactionId() string
	WALSequence() int64
	WALTimestamp() int64
	WriteJson(w io.Writer) error
}
