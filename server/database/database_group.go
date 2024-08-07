package database

import (
	"sync"
	"time"
)

type DatabaseGroup struct {
	checkpointedAt      time.Time
	branches            map[string][]*BranchConnection
	branchWalSha256     map[string][32]byte
	branchWalTimestamps map[string]int64
	locks               map[string]*sync.RWMutex
	lockMutex           *sync.RWMutex
}

func NewDatabaseGroup() *DatabaseGroup {
	return &DatabaseGroup{
		branches:            map[string][]*BranchConnection{},
		branchWalSha256:     map[string][32]byte{},
		branchWalTimestamps: map[string]int64{},
		locks:               map[string]*sync.RWMutex{},
		lockMutex:           &sync.RWMutex{},
	}
}
