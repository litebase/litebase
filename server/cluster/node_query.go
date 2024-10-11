package cluster

import "io"

type NodeQueryBuilder interface {
	Build(
		accessKeyId string,
		databaseHash string,
		databaseId string,
		branchId string,
		statement string,
		parameters []interface{},
		id string,
	) (NodeQuery, error)
}

type NodeQuery interface {
	Resolve(response NodeQueryResponse) error
}

type NodeQueryResponse interface {
	ToMap() map[string]interface{}
	ToJSON() ([]byte, error)
	WriteJson(w io.Writer) error
}
