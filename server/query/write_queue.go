package query

import (
	"context"
	"sync"
	"sync/atomic"
)

var writeQueues = sync.Map{}

type WriteQueue struct {
	branchUuid        string
	context           context.Context
	databaseUuid      string
	jobs              chan WriteQueueJob
	resultChannelPool sync.Pool
	running           *int32
}

type WriteQueueJob struct {
	context context.Context
	handler func(f func(query *Query) (QueryResponse, error), query *Query) (QueryResponse, error)
	f       func(query *Query) (QueryResponse, error)
	query   *Query
	result  chan WriteQueueResult
}

type WriteQueueResult struct {
	err           error
	queryResponse QueryResponse
}

func GetWriteQueue(query *Query) *WriteQueue {
	ctx := context.TODO()

	if writeQueue, ok := writeQueues.Load(query.DatabaseKey.DatabaseHash); ok {
		return writeQueue.(*WriteQueue)
	}

	writeQueue := &WriteQueue{
		branchUuid:   query.DatabaseKey.BranchUuid,
		context:      ctx,
		databaseUuid: query.DatabaseKey.DatabaseUuid,
		jobs:         make(chan WriteQueueJob),
		running:      new(int32),
		resultChannelPool: sync.Pool{
			New: func() interface{} {
				return make(chan WriteQueueResult)
			},
		},
	}

	writeQueues.Store(query.DatabaseKey.DatabaseHash, writeQueue)

	return writeQueue
}

func (wq *WriteQueue) Handle(handler func(f func(query *Query) (QueryResponse, error), query *Query) (QueryResponse, error), f func(query *Query) (QueryResponse, error), query *Query) (QueryResponse, error) {
	if !wq.isRunning() {
		atomic.StoreInt32(wq.running, 1)
		go wq.processQueue()
	}

	resultChannel := wq.resultChannelGet()
	defer wq.resultChannelPut(resultChannel)

	wq.jobs <- WriteQueueJob{
		context: wq.context,
		f:       f,
		handler: handler,
		query:   query,
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
			queryResponse, err := job.handler(job.f, job.query)

			job.result <- WriteQueueResult{
				err:           err,
				queryResponse: queryResponse,
			}

			// close(job.result)
		}
	}
}

func (wq *WriteQueue) resultChannelGet() chan WriteQueueResult {
	return wq.resultChannelPool.Get().(chan WriteQueueResult)
}

func (wq *WriteQueue) resultChannelPut(resultChannel chan WriteQueueResult) {
	select {
	case <-resultChannel:
	default:
	}

	wq.resultChannelPool.Put(resultChannel)
}
