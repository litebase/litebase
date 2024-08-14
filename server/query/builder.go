package query

import (
	"litebase/server/auth"
	"litebase/server/database"
	"litebase/server/node"
)

type QueryBuilder struct{}

type QueryType interface {
	Resolve() (node.NodeQueryResponse, error)
}

func NewQueryBuilder() *QueryBuilder {
	return &QueryBuilder{}
}

func (qb *QueryBuilder) Build(
	accessKeyId string,
	databaseHash string,
	databaseUuid string,
	branchUuid string,
	statement string,
	parameters []interface{},
	id string,
) (node.NodeQuery, error) {
	accessKey, err := auth.AccessKeyManager().Get(accessKeyId)

	if err != nil {
		return &Query{}, err
	}

	return NewQuery(
		database.NewDatabaseKey(databaseUuid, branchUuid),
		accessKey,
		&QueryInput{
			Statement:  statement,
			Parameters: parameters,
			Id:         id,
		},
	)
}
