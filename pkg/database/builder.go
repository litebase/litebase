package database

import (
	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/cluster"
	"github.com/litebase/litebase/pkg/logs"
	"github.com/litebase/litebase/pkg/sqlite3"
)

type QueryBuilder struct {
	auth            *auth.Auth
	cluster         *cluster.Cluster
	databaseManager *DatabaseManager
	logManager      *logs.LogManager
}

type QueryType interface {
	Resolve() (cluster.NodeQueryResponse, error)
}

func NewQueryBuilder(
	cluster *cluster.Cluster,
	auth *auth.Auth,
	databaseManager *DatabaseManager,
	logManager *logs.LogManager,
) *QueryBuilder {
	return &QueryBuilder{
		auth:            auth,
		cluster:         cluster,
		databaseManager: databaseManager,
		logManager:      logManager,
	}
}

func (qb *QueryBuilder) Build(
	credentialID string,
	credentialScheme string,
	databaseId string,
	databaseName string,
	branchId string,
	branchName string,
	statement string,
	parameters []sqlite3.StatementParameter,
	id string,
) (cluster.NodeQuery, error) {
	credential, err := qb.auth.GetCredential(credentialID, credentialScheme)

	if err != nil {
		return &Query{}, err
	}

	return NewQuery(
		qb.cluster,
		qb.databaseManager,
		qb.logManager,
		auth.NewDatabaseKey(databaseId, databaseName, branchId, branchName),
		credential,
		&QueryInput{
			ID:         id,
			Parameters: parameters,
			Statement:  statement,
		},
	)
}
