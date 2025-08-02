package database

import (
	"context"
	"sync"
	"time"
)

type WriteQueueManager struct {
	context context.Context
	queues  *sync.Map
}

// NewWriteQueueManager creates a new write queue manager instance.
func NewWriteQueueManager(context context.Context) *WriteQueueManager {
	return &WriteQueueManager{
		context: context,
		queues:  &sync.Map{},
	}
}

// GetWriteQueue returns the write queue that matches the database of the query.
func (wqm *WriteQueueManager) GetWriteQueue(query *Query) *WriteQueue {
	ctx := context.Background()

	if writeQueue, ok := wqm.queues.Load(query.DatabaseKey.DatabaseHash); ok {
		return writeQueue.(*WriteQueue)
	}

	writeQueue := &WriteQueue{
		branchId:   query.DatabaseKey.DatabaseBranchID,
		context:    ctx,
		databaseId: query.DatabaseKey.DatabaseID,
		// Setup a buffered channel to hold up to 1000 concurrent jobs
		jobs:  make(chan WriteQueueJob, 1),
		mutex: sync.Mutex{},
		resultChannelPool: sync.Pool{
			New: func() any {
				return make(chan *WriteQueueResult)
			},
		},
		resultPool: sync.Pool{
			New: func() interface{} {
				return &WriteQueueResult{}
			},
		},
		running: false,
	}

	wqm.queues.Store(query.DatabaseKey.DatabaseHash, writeQueue)

	return writeQueue
}

// Run checks the write queues every second to see if they are idle. If they are
// idle, the write queue is stopped.
func (wqm *WriteQueueManager) Run() {
	ticker := time.NewTicker(1 * time.Second)

	for {
		select {
		case <-wqm.context.Done():
			return
		case <-ticker.C:
			wqm.queues.Range(func(key, value interface{}) bool {
				// Stop the write queue if it is not running
				if !value.(*WriteQueue).running {
					return true
				}

				if value.(*WriteQueue).isIdle() {
					value.(*WriteQueue).stop()
				}

				return true
			})
		}
	}
}
