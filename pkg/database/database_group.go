package database

import (
	"sync"
	"time"
)

type DatabaseGroup struct {
	checkpointedAt time.Time
	branches       map[string][]*BranchConnection
	mutex          *sync.Mutex
}

func NewDatabaseGroup() *DatabaseGroup {
	return &DatabaseGroup{
		branches: map[string][]*BranchConnection{},
		mutex:    &sync.Mutex{},
	}
}
