package database

import (
	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/cluster"
	"github.com/litebase/litebase/pkg/logs"
	"github.com/litebase/litebase/pkg/sqlite3"
)

type QueryBuilder struct {
	accessKeyManager *auth.AccessKeyManager
	cluster          *cluster.Cluster
	databaseManager  *DatabaseManager
	logManager       *logs.LogManager
}

type QueryType interface {
	Resolve() (cluster.NodeQueryResponse, error)
}

func NewQueryBuilder(
	cluster *cluster.Cluster,
	accessKeyManager *auth.AccessKeyManager,
	databaseManager *DatabaseManager,
	logManager *logs.LogManager,
) *QueryBuilder {
	return &QueryBuilder{
		accessKeyManager: accessKeyManager,
		cluster:          cluster,
		databaseManager:  databaseManager,
		logManager:       logManager,
	}
}

func (qb *QueryBuilder) Build(
	accessKeyId string,
	databaseId string,
	databaseName string,
	branchId string,
	branchName string,
	statement string,
	parameters []sqlite3.StatementParameter,
	id string,
) (cluster.NodeQuery, error) {
	accessKey, err := qb.accessKeyManager.Get(accessKeyId)

	if err != nil {
		return &Query{}, err
	}

	return NewQuery(
		qb.cluster,
		qb.databaseManager,
		qb.logManager,
		auth.NewDatabaseKey(databaseId, databaseName, branchId, branchName),
		accessKey,
		&QueryInput{
			ID:         id,
			Parameters: parameters,
			Statement:  statement,
		},
	)
}
