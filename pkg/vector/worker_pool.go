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
	StreamChan  chan *ChunkResult
}

// ChunkResult represents the result of processing a chunk
type ChunkResult struct {
	ChunkID int
	Heap    *TopKHeap
	Error   error
}

// Worker represents a single worker with optional prefetch support
type Worker struct {
	id           int
	prefetchChan chan *ChunkResult // Channel for prefetched results (Phase 2 optimization)
}

// WorkerPool manages a pool of goroutines for parallel processing
type WorkerPool struct {
	maxWorkers int
	jobChan    chan *ChunkJob
	workers    []*Worker
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
		workers:    make([]*Worker, maxWorkers),
		ctx:        ctx,
		cancel:     cancel,
	}

	for i := 0; i < maxWorkers; i++ {
		pool.workers[i] = &Worker{
			id:           i,
			prefetchChan: make(chan *ChunkResult, 1), // Buffered for async prefetch
		}

		pool.wg.Add(1)

		go pool.worker(pool.workers[i])
	}

	return pool
}

// worker processes jobs from the job channel with prefetching support
// Phase 2 optimization: Overlaps I/O (prefetch next chunk) with compute (process current chunk)
func (wp *WorkerPool) worker(w *Worker) {
	defer wp.wg.Done()
	defer func() {
		if r := recover(); r != nil {
			slog.Error("Worker panic recovered", "worker_id", w.id, "panic", r)
		}
	}()

	var prefetchedJob *ChunkJob
	var prefetchedResult *ChunkResult

	for {
		var currentJob *ChunkJob
		var currentResult *ChunkResult

		// If we have a prefetched result, use it
		if prefetchedJob != nil {
			currentJob = prefetchedJob
			currentResult = prefetchedResult
			prefetchedJob = nil
			prefetchedResult = nil
		} else {
			// Otherwise, get the next job from channel
			select {
			case <-wp.ctx.Done():
				return
			case job, ok := <-wp.jobChan:
				if !ok {
					return
				}
				currentJob = job
				currentResult = nil // Will be computed below
			}
		}

		// Try to prefetch next job while processing current
		// Use non-blocking select to avoid waiting
		select {
		case nextJob, ok := <-wp.jobChan:
			if ok {
				// Start prefetching asynchronously
				go func(job *ChunkJob) {
					result := wp.executePrefetch(w, job)
					// Send result to worker's prefetch channel
					select {
					case w.prefetchChan <- result:
					case <-wp.ctx.Done():
						// Context cancelled, send error result
						job.ResultChan <- &ChunkResult{
							ChunkID: job.ChunkID,
							Error:   context.Canceled,
						}
					}
				}(nextJob)
				prefetchedJob = nextJob
			}
		default:
			// No job available, continue without prefetch
		}

		// Process current job if we haven't already
		if currentResult == nil {
			currentResult = wp.executeJob(w, currentJob)
		}

		// Send result
		currentJob.ResultChan <- currentResult

		// If we started a prefetch, wait for it to complete
		if prefetchedJob != nil {
			select {
			case prefetchedResult = <-w.prefetchChan:
				// Prefetch completed, will use on next iteration
			case <-wp.ctx.Done():
				return
			}
		}
	}
}

// executeJob executes a job and returns the result (with panic recovery)
func (wp *WorkerPool) executeJob(w *Worker, job *ChunkJob) *ChunkResult {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("Job execution panic", "worker_id", w.id, "chunk_id", job.ChunkID, "panic", r)
		}
	}()

	result, err := ExecuteChunkScanWithWorker(w, job)

	if err != nil {
		return &ChunkResult{
			ChunkID: job.ChunkID,
			Error:   err,
		}
	}

	return result
}

// executePrefetch executes a prefetch job (same as executeJob but for async prefetch)
func (wp *WorkerPool) executePrefetch(w *Worker, job *ChunkJob) *ChunkResult {
	return wp.executeJob(w, job)
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
