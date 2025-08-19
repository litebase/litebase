package cluster

import (
	"github.com/litebase/litebase/pkg/sqlite3"
)

type NodeQueryBuilder interface {
	Build(
		credentialID string,
		credentialScheme string,
		databaseId string,
		databaseName string,
		branchId string,
		branchName string,
		statement string,
		parameters []sqlite3.StatementParameter,
		id string,
	) (NodeQuery, error)
}

type NodeQuery interface {
	Resolve(response NodeQueryResponse) (NodeQueryResponse, error)
}
