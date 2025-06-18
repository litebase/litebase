package cluster

import (
	"github.com/litebase/litebase/pkg/sqlite3"
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
	Resolve(response NodeQueryResponse) (NodeQueryResponse, error)
}
