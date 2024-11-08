package database

import (
	"context"
	"sync"
	"time"
)

const WriteQueueCapacity = 1000

// The WriteQueue is a queue for handling write queries for a database.
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

// A WriteQueueJob is a container for a query to be handled by the WriteQueue.
type WriteQueueJob struct {
	context  context.Context
	handler  func(f func(query *Query, response *QueryResponse) (*QueryResponse, error), query *Query, response *QueryResponse) (*QueryResponse, error)
	f        func(query *Query, response *QueryResponse) (*QueryResponse, error)
	query    *Query
	response *QueryResponse
	result   chan *WriteQueueResult
}

type WriteQueueResult struct {
	err error
}

// Handle a query with the WriteQueue.
func (wq *WriteQueue) Handle(
	handler func(
		f func(query *Query, response *QueryResponse) (*QueryResponse, error),
		query *Query,
		response *QueryResponse,
	) (*QueryResponse, error),
	f func(query *Query, response *QueryResponse) (*QueryResponse, error),
	query *Query,
	response *QueryResponse,
) (*QueryResponse, error) {
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

	return response, res.err
}

// Detect if the WriteQueue is idle.
func (wq *WriteQueue) isIdle() bool {
	return time.Since(wq.activeAt) > 3*time.Second
}

// Process the jobs on the queue.
func (wq *WriteQueue) processQueue() {
	for job := range wq.jobs {
		// Process the job immediately
		_, err := job.handler(job.f, job.query, job.response)

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

// Get a result channel from the pool.
func (wq *WriteQueue) resultChannelGet() chan *WriteQueueResult {
	return wq.resultChannelPool.Get().(chan *WriteQueueResult)
}

// Put a result channel back into the pool.
func (wq *WriteQueue) resultChannelPut(resultChannel chan *WriteQueueResult) {
	select {
	case <-resultChannel:
	default:
	}

	wq.resultChannelPool.Put(resultChannel)
}

// Start the WriteQueue.
func (wq *WriteQueue) start() {
	wq.running = true
	go wq.processQueue()
}

// Stop the WriteQueue.
func (wq *WriteQueue) stop() {
	wq.running = false
}
