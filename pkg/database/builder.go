package database

import (
	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/cluster"
	"github.com/litebase/litebase/pkg/sqlite3"
	"github.com/litebase/litebase/server/logs"
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
	databaseKey string,
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
		qb.logManager,
		auth.NewDatabaseKey(databaseId, branchId, databaseKey),
		accessKey,
		&QueryInput{
			Id:         id,
			Parameters: parameters,
			Statement:  statement,
		},
	)
}
