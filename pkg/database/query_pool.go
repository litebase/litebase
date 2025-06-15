package database

import (
	"sync"

	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/cluster"
	"github.com/litebase/litebase/pkg/logs"
)

var queryPool = &sync.Pool{
	New: func() interface{} {
		return &Query{}
	},
}

func Pool() *sync.Pool {
	return queryPool
}

func GetQuery(
	cluster *cluster.Cluster,
	databaseManager *DatabaseManager,
	logManager *logs.LogManager,
	databaseKey *auth.DatabaseKey,
	accessKey *auth.AccessKey,
	input *QueryInput,
) *Query {
	query := queryPool.Get().(*Query)

	query.Reset()

	query.AccessKey = accessKey
	query.DatabaseKey = databaseKey
	query.Input = input
	query.cluster = cluster
	query.databaseManager = databaseManager
	query.logManager = logManager

	return query
}

func PutQuery(query *Query) {
	query.Reset()
	queryPool.Put(query)
}
