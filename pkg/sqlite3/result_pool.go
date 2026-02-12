package sqlite3

import (
	"sync"
)

const (
	// maxResultRows is the maximum number of rows a Result can have to be returned to the pool
	// Results with larger capacities will be discarded to prevent unbounded memory growth
	maxResultRows = 1000
	// maxResultColumns is the maximum number of columns a Result can have to be returned to the pool
	maxResultColumns = 100
)

// Result represents a result of a SQLite query that can be reused for memory efficiency.
type ResultPool struct {
	results *sync.Pool
}

// Create a new ResultPool instance
func NewResultPool() *ResultPool {
	return &ResultPool{
		results: &sync.Pool{
			New: func() any {
				return NewResult()
			},
		},
	}
}

// Get a Result from the pool
func (rp *ResultPool) Get() *Result {
	return rp.results.Get().(*Result)
}

// Put a Result back into the pool
func (rp *ResultPool) Put(r *Result) {
	if r == nil {
		return
	}

	// Ensure internal buffers/columns/rows are released back to their pools
	r.Reset()

	// Only return reasonably-sized Results to pool to prevent unbounded memory growth
	// Check capacity of internal slices - even though length is 0 after Reset(),
	// the underlying arrays can be huge
	if cap(r.Rows) <= maxResultRows && 
	   cap(r.Columns) <= maxResultColumns && 
	   cap(r.ColumnTypes) <= maxResultColumns {
		rp.results.Put(r)
	}
	// Oversized Results are discarded and will be garbage collected
}
