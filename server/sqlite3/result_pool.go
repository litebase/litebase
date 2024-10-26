package sqlite3

import (
	"sync"
)

// Result represents a result of a SQLite query that can be reused for memory efficiency.
type ResultPool struct {
	results *sync.Pool
}

// Create a new ResultPool instance
func NewResultPool() *ResultPool {
	return &ResultPool{
		results: &sync.Pool{
			New: func() interface{} {
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
	rp.results.Put(r)
}
