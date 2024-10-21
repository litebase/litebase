package database

import (
	"litebase/server/auth"
	"litebase/server/cluster"
	"sync"
)

var queryPool = &sync.Pool{
	New: func() interface{} {
		return &Query{}
	},
}

func Pool() *sync.Pool {
	return queryPool
}

func Get(
	cluster *cluster.Cluster,
	databaseManager *DatabaseManager,
	databaseKey *DatabaseKey,
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

	return query
}

func Put(query *Query) {
	query.Reset()
	queryPool.Put(query)
}
