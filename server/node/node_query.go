package node

import "io"

type NodeQueryBuilder interface {
	Build(
		accessKeyId string,
		databaseUuid string,
		branchUuid string,
		statement string,
		parameters []interface{},
		id string,
	) (NodeQuery, error)
}

type NodeQuery interface {
	Resolve(databaseHash string) (NodeQueryResponse, error)
}

type NodeQueryResponse interface {
	ToMap() map[string]interface{}
	ToJSON() ([]byte, error)
	WriteJson(w io.Writer) error
}
