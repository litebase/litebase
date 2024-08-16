package query

import (
	"context"
	"sync"
	"sync/atomic"
)

var writeQueues = sync.Map{}

type WriteQueue struct {
	branchUuid        string
	capacity          int
	context           context.Context
	databaseUuid      string
	jobs              chan WriteQueueJob
	resultPool        *sync.Pool
	resultChannelPool *sync.Pool
	running           *int32
}

type WriteQueueJob struct {
	context  context.Context
	handler  func(f func(query *Query, response *QueryResponse) error, query *Query, response *QueryResponse) error
	f        func(query *Query, response *QueryResponse) error
	query    *Query
	response *QueryResponse
	result   chan *WriteQueueResult
}

type WriteQueueResult struct {
	err error
}

func GetWriteQueue(query *Query) *WriteQueue {
	ctx := context.TODO()

	if writeQueue, ok := writeQueues.Load(query.DatabaseKey.DatabaseHash); ok {
		return writeQueue.(*WriteQueue)
	}

	writeQueue := &WriteQueue{
		capacity:     1000,
		branchUuid:   query.DatabaseKey.BranchUuid,
		context:      ctx,
		databaseUuid: query.DatabaseKey.DatabaseUuid,
		// Setup a buffered channel to hold up to 1000 concurrent jobs
		jobs:    make(chan WriteQueueJob, 1000),
		running: new(int32),
		resultChannelPool: &sync.Pool{
			New: func() interface{} {
				return make(chan *WriteQueueResult)
			},
		},
		resultPool: &sync.Pool{
			New: func() interface{} {
				return &WriteQueueResult{}
			},
		},
	}

	writeQueues.Store(query.DatabaseKey.DatabaseHash, writeQueue)

	return writeQueue
}

func (wq *WriteQueue) Handle(
	handler func(f func(query *Query, response *QueryResponse) error, query *Query, response *QueryResponse) error,
	f func(query *Query, response *QueryResponse) error,
	query *Query,
	response *QueryResponse,
) error {
	if !wq.isRunning() {
		atomic.StoreInt32(wq.running, 1)
		go wq.processQueue()
	}

	resultChannel := wq.resultChannelGet()
	defer wq.resultChannelPut(resultChannel)

	wq.jobs <- WriteQueueJob{
		context:  wq.context,
		f:        f,
		handler:  handler,
		query:    query,
		response: response,
		result:   resultChannel,
	}

	// for {
	select {
	case <-wq.context.Done():
		return wq.context.Err()
	case res := <-resultChannel:
		return res.err
	}
}

func (wq *WriteQueue) isRunning() bool {
	return atomic.LoadInt32(wq.running) == 1
}

func (wq *WriteQueue) processQueue() {
	// var job WriteQueueJob
	capacity := wq.capacity
	localQueue := make([]WriteQueueJob, capacity)
	queueIndex := 0

	for {
		select {
		case <-wq.context.Done():
			atomic.StoreInt32(wq.running, 0)
			return
		default:
		pullJobs:
			for queueIndex < capacity {
				select {
				case job := <-wq.jobs:
					localQueue[queueIndex] = job
					queueIndex++
				default:
					break pullJobs
				}
			}

			// Process jobs from the local queue one at a time
			if queueIndex > 0 {
				job := localQueue[0]
				copy(localQueue[0:], localQueue[1:queueIndex])
				queueIndex--

				err := job.handler(job.f, job.query, job.response)

				result := wq.resultPool.Get().(*WriteQueueResult)
				result.err = err
				job.result <- result
				wq.resultPool.Put(result)
			}
		}
	}
}

func (wq *WriteQueue) resultChannelGet() chan *WriteQueueResult {
	return wq.resultChannelPool.Get().(chan *WriteQueueResult)
}

func (wq *WriteQueue) resultChannelPut(resultChannel chan *WriteQueueResult) {
	select {
	case <-resultChannel:
	default:
	}

	wq.resultChannelPool.Put(resultChannel)
}
