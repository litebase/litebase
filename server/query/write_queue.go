package query

import (
	"context"
	"sync"
	"sync/atomic"
)

var writeQueues = sync.Map{}

type WriteQueue struct {
	branchUuid   string
	context      context.Context
	databaseUuid string
	jobs         chan WriteQueueJob
	running      *int32
}

type WriteQueueJob struct {
	context context.Context
	handler func() (QueryResponse, error)
	result  chan WriteQueueResult
}

type WriteQueueResult struct {
	err           error
	queryResponse QueryResponse
}

func GetWriteQueue(databaseHash, databaseUuid, branchUuid string) *WriteQueue {
	ctx := context.TODO()

	if writeQueue, ok := writeQueues.Load(databaseHash); ok {
		return writeQueue.(*WriteQueue)
	}

	writeQueue := &WriteQueue{
		branchUuid:   branchUuid,
		context:      ctx,
		databaseUuid: databaseUuid,
		jobs:         make(chan WriteQueueJob),
		running:      new(int32),
	}

	writeQueues.Store(databaseHash, writeQueue)

	return writeQueue
}

func (wq *WriteQueue) Handle(handler func() (QueryResponse, error)) (QueryResponse, error) {
	if !wq.isRunning() {
		atomic.StoreInt32(wq.running, 1)
		go wq.processQueue()
	}

	resultChannel := make(chan WriteQueueResult)

	wq.jobs <- WriteQueueJob{
		context: wq.context,
		handler: handler,
		result:  resultChannel,
	}

	// for {
	select {
	case <-wq.context.Done():
		return QueryResponse{}, wq.context.Err()
	case res := <-resultChannel:
		return res.queryResponse, res.err
	}
}

func (wq *WriteQueue) isRunning() bool {
	return atomic.LoadInt32(wq.running) == 1
}

func (wq *WriteQueue) processQueue() {
	var job WriteQueueJob

	for {
		select {
		case <-wq.context.Done():
			atomic.StoreInt32(wq.running, 0)
			return
		case job = <-wq.jobs:
			queryResponse, err := job.handler()

			job.result <- WriteQueueResult{
				err:           err,
				queryResponse: queryResponse,
			}

			// close(job.result)
		}
	}
}
