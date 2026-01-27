package vector

import (
	"context"
	"log/slog"
	"sync"
)

// ChunkJob represents a chunk of work for the worker pool
type ChunkJob struct {
	ChunkID     int
	StartRow    int64
	EndRow      int64
	VfsID       string
	DatabaseID  string
	BranchID    string
	TableName   string
	ColumnName  string
	QueryVector *VectorBlob
	Metric      string
	K           int
	ResultChan  chan *ChunkResult
}

// ChunkResult represents the result of processing a chunk
type ChunkResult struct {
	ChunkID int
	Heap    *TopKHeap
	Error   error
}

// WorkerPool manages a pool of goroutines for parallel processing
type WorkerPool struct {
	maxWorkers int
	jobChan    chan *ChunkJob
	wg         sync.WaitGroup
	ctx        context.Context
	cancel     context.CancelFunc
}

// NewWorkerPool creates a new worker pool
func NewWorkerPool(maxWorkers int) *WorkerPool {
	ctx, cancel := context.WithCancel(context.Background())

	pool := &WorkerPool{
		maxWorkers: maxWorkers,
		jobChan:    make(chan *ChunkJob, maxWorkers*2),
		ctx:        ctx,
		cancel:     cancel,
	}

	for i := 0; i < maxWorkers; i++ {
		pool.wg.Add(1)

		go pool.worker(i)
	}

	return pool
}

// worker processes jobs from the job channel
func (wp *WorkerPool) worker(id int) {
	defer wp.wg.Done()
	defer func() {
		if r := recover(); r != nil {
			slog.Error("Worker panic recovered", "worker_id", id, "panic", r)
		}
	}()

	for {
		select {
		case <-wp.ctx.Done():
			return
		case job, ok := <-wp.jobChan:
			if !ok {
				return
			}

			wp.processJob(job)
		}
	}
}

// processJob processes a single chunk job
func (wp *WorkerPool) processJob(job *ChunkJob) {
	defer func() {
		if r := recover(); r != nil {
			job.ResultChan <- &ChunkResult{
				ChunkID: job.ChunkID,
				Error:   ErrInvalidBlobFormat,
			}
		}
	}()

	result, err := ExecuteChunkScan(job)

	if err != nil {
		job.ResultChan <- &ChunkResult{
			ChunkID: job.ChunkID,
			Error:   err,
		}

		return
	}

	job.ResultChan <- result
}

func (wp *WorkerPool) MaxWorkers() int {
	return wp.maxWorkers
}

// Submit submits a job to the worker pool
func (wp *WorkerPool) Submit(job *ChunkJob) {
	select {
	case <-wp.ctx.Done():
		job.ResultChan <- &ChunkResult{
			ChunkID: job.ChunkID,
			Error:   context.Canceled,
		}
	case wp.jobChan <- job:
		// Job submitted successfully
	}
}

// Shutdown gracefully shuts down the worker pool
func (wp *WorkerPool) Shutdown() {
	wp.cancel()
	close(wp.jobChan)
	wp.wg.Wait()
}
