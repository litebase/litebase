package query

type QueryInput struct {
	Id         string        `json:"id"`
	Statement  string        `json:"statement"`
	Parameters []interface{} `json:"parameters"`
}
