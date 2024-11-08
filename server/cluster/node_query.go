package cluster

import (
	"io"
	"litebase/server/sqlite3"
)

type NodeQueryBuilder interface {
	Build(
		accessKeyId string,
		databaseHash string,
		databaseId string,
		branchId string,
		statement []byte,
		parameters []sqlite3.StatementParameter,
		id []byte,
	) (NodeQuery, error)
}

type NodeQuery interface {
	Resolve(response NodeQueryResponse) (NodeQueryResponse, error)
}

type NodeQueryResponse interface {
	Changes() int64
	Columns() []string
	LastInsertRowId() int64
	Latency() float64
	RowCount() int
	Rows() [][]*sqlite3.Column

	ToMap() map[string]interface{}
	ToJSON() ([]byte, error)
	WriteJson(w io.Writer) error
}
