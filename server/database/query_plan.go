package database

type QueryPlan struct {
	Table string
}

func NewQueryPlan() QueryPlan {
	return QueryPlan{}
}
