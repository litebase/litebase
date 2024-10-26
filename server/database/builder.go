package database

import (
	"litebase/server/auth"
	"litebase/server/cluster"
	"litebase/server/sqlite3"
)

type QueryBuilder struct {
	accessKeyManager *auth.AccessKeyManager
	cluster          *cluster.Cluster
	databaseManager  *DatabaseManager
}

type QueryType interface {
	Resolve() (cluster.NodeQueryResponse, error)
}

func NewQueryBuilder(
	cluster *cluster.Cluster,
	accessKeyManager *auth.AccessKeyManager,
	databaseManager *DatabaseManager,
) *QueryBuilder {
	return &QueryBuilder{
		accessKeyManager: accessKeyManager,
		cluster:          cluster,
		databaseManager:  databaseManager,
	}
}

func (qb *QueryBuilder) Build(
	accessKeyId string,
	databaseHash string,
	databaseId string,
	branchId string,
	statement []byte,
	parameters []sqlite3.StatementParameter,
	id []byte,
) (cluster.NodeQuery, error) {
	accessKey, err := qb.accessKeyManager.Get(accessKeyId)

	if err != nil {
		return &Query{}, err
	}

	return NewQuery(
		qb.cluster,
		qb.databaseManager,
		NewDatabaseKey(databaseId, branchId),
		accessKey,
		&QueryInput{
			Statement:  statement,
			Parameters: parameters,
			Id:         id,
		},
	)
}
