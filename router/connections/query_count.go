package connections

/*
This struct represents a count of queries for a database at a given time.
*/
type QueryCount struct {
	databaseKey string
	timestamp   int64
	Count       int
}

/*
Create a new instance of the query count.
*/
func NewQueryCount(databaseKey string, timestamp int64) *QueryCount {
	return &QueryCount{
		databaseKey: databaseKey,
		timestamp:   timestamp,
		Count:       0,
	}
}

/*
Increment the query count value.
*/
func (c *QueryCount) Increment() {
	c.Count++
}
