package database

import (
	"sync"
	"time"
)

type DatabaseGroup struct {
	checkpointedAt time.Time
	branches       map[string][]*BranchConnection
	locks          map[string]*sync.RWMutex
	lockMutex      *sync.RWMutex
}

func NewDatabaseGroup() *DatabaseGroup {
	return &DatabaseGroup{
		branches:  map[string][]*BranchConnection{},
		locks:     map[string]*sync.RWMutex{},
		lockMutex: &sync.RWMutex{},
	}
}
