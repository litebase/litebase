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
		statement string,
		parameters []sqlite3.StatementParameter,
		id string,
	) (NodeQuery, error)
}

type NodeQuery interface {
	Resolve(response NodeQueryResponse) error
}

type NodeQueryResponse interface {
	ToMap() map[string]interface{}
	ToJSON() ([]byte, error)
	WriteJson(w io.Writer) error
}
