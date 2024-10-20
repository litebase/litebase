package counter

// This struct represents a count of queries for a database at a given time.
type QueryCount struct {
	Count     int
	Key       string
	Timestamp int64
}

// Increment the query count value.
func (counter *QueryCount) Increment() {
	counter.Count++
}
