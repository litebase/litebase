package cluster

import (
	"github.com/litebase/litebase/server/sqlite3"
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
