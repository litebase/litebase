package database

import "sync"

var QueryInputPool = &sync.Pool{
	New: func() interface{} {
		return NewQueryInput(nil, nil, nil, nil)
	},
}
