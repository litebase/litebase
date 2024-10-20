package query

import (
	"context"
	"sync"
	"time"
)

const WriteQueueCapacity = 1000

type WriteQueue struct {
	activeAt          time.Time
	branchId          string
	context           context.Context
	databaseId        string
	jobs              chan WriteQueueJob
	mutex             sync.Mutex
	resultPool        sync.Pool
	resultChannelPool sync.Pool
	running           bool
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

	if writeQueue, ok := writeQueueManager.queues.Load(query.DatabaseKey.DatabaseHash); ok {
		return writeQueue.(*WriteQueue)
	}

	writeQueue := &WriteQueue{
		branchId:   query.DatabaseKey.BranchId,
		context:    ctx,
		databaseId: query.DatabaseKey.DatabaseId,
		// Setup a buffered channel to hold up to 1000 concurrent jobs
		jobs:  make(chan WriteQueueJob, WriteQueueCapacity),
		mutex: sync.Mutex{},
		resultChannelPool: sync.Pool{
			New: func() interface{} {
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

	writeQueueManager.queues.Store(query.DatabaseKey.DatabaseHash, writeQueue)

	return writeQueue
}

func (wq *WriteQueue) Handle(
	handler func(
		f func(query *Query, response *QueryResponse) error,
		query *Query,
		response *QueryResponse,
	) error,
	f func(query *Query, response *QueryResponse) error,
	query *Query,
	response *QueryResponse,
) error {
	if !wq.running {
		wq.mutex.Lock()
		shouldStart := !wq.running

		if shouldStart {
			wq.start()
		}

		wq.mutex.Unlock()
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

	res := <-resultChannel

	return res.err
}

func (wq *WriteQueue) isIdle() bool {
	return time.Since(wq.activeAt) > 3*time.Second
}

func (wq *WriteQueue) processQueue() {
	for job := range wq.jobs {
		// Process the job immediately
		err := job.handler(job.f, job.query, job.response)

		// Send the result back
		result := wq.resultPool.Get().(*WriteQueueResult)
		result.err = err
		job.result <- result
		wq.resultPool.Put(result)
		wq.activeAt = time.Now()

		// Check if we should stop running
		if !wq.running {
			return
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

func (wq *WriteQueue) start() {
	wq.running = true
	go wq.processQueue()
}

func (wq *WriteQueue) stop() {
	wq.running = false
}
