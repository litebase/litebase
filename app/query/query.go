package query

import (
	"encoding/json"

	"github.com/google/uuid"
)

type Query struct {
	Id         string  `json:"id"`
	Batch      []Query `json:"batch"`
	Statement  string  `json:"statement"`
	Parameters []any   `json:"parameters"`
}

func New(data []byte, id string) Query {
	var query = Query{}
	json.Unmarshal(data, &query)
	if id == "" {
		uuid, _ := uuid.NewUUID()
		query.Id = uuid.String()
	} else {
		query.Id = id
	}

	return query
}
