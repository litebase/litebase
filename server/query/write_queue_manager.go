package query

import (
	"sync"
	"time"
)

var writeQueueManager = NewWriteQueueManager()

func init() {
	go writeQueueManager.Run()
}

type WriteQueueManager struct {
	queues sync.Map
}

func NewWriteQueueManager() *WriteQueueManager {
	return &WriteQueueManager{
		queues: sync.Map{},
	}
}

func (wqm *WriteQueueManager) Run() {
	ticker := time.NewTicker(1 * time.Second)

	for range ticker.C {
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
