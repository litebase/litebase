package queue

import (
	"context"
	"fmt"
	"log/slog"
	"runtime"
	"sync"
	"time"

	"github.com/litebase/litebase/pkg/cluster"
	"github.com/litebase/litebase/pkg/database"
)

// WorkerPool manages a pool of workers that process jobs from the queue.
type WorkerPool struct {
	workers          []*Worker
	systemDB         *database.SystemDatabase
	cluster          *cluster.Cluster
	registry         *JobRegistry
	batchManager     *BatchManager
	workerCount      int
	started          bool
	primaryOnly      bool
	runningJobKeys   sync.Map   // Tracks currently running job keys to prevent overlap
	reservationMutex sync.Mutex // Serializes job reservation to prevent DB locks
}

// WorkerPoolConfig configures the worker pool.
type WorkerPoolConfig struct {
	// WorkerCount is the number of workers to create.
	// If 0, defaults to number of CPUs / 2.
	WorkerCount int

	// PrimaryOnly determines if workers should only run on primary nodes.
	PrimaryOnly bool
}

// NewWorkerPool creates a new worker pool.
func NewWorkerPool(systemDB *database.SystemDatabase, cluster *cluster.Cluster, config WorkerPoolConfig) *WorkerPool {
	workerCount := config.WorkerCount

	if workerCount == 0 {
		workerCount = max(runtime.NumCPU()/2, 1)
	}

	pool := &WorkerPool{
		systemDB:    systemDB,
		cluster:     cluster,
		registry:    NewJobRegistry(),
		workerCount: workerCount,
		primaryOnly: config.PrimaryOnly,
	}

	// Create batch manager (dispatcher will be set when NewDispatcher is called)
	pool.batchManager = NewBatchManager(systemDB)

	return pool
}

// RegisterJob registers a new job type with its handler and configuration.
func (p *WorkerPool) RegisterJob(name string, handler JobHandler, opts ...JobOption) error {
	config := &JobTypeConfig{
		Name:       name,
		QueueName:  "default",
		Retries:    3,
		RetryAfter: 30 * time.Second,
		Handler:    handler,
	}

	for _, opt := range opts {
		opt(config)
	}

	if config.Handler == nil {
		return fmt.Errorf("job handler is required for job type %s", name)
	}

	// Create a prototype job
	prototype := &ConfiguredJob{
		name:              config.Name,
		queueName:         config.QueueName,
		retries:           config.Retries,
		retryAfter:        config.RetryAfter,
		handler:           config.Handler,
		throttleFunc:      config.Throttle,
		withoutOverlap:    config.WithoutOverlap,
		overlapRetryDelay: config.OverlapRetryDelay,
		timeout:           config.Timeout,
		data:              make(map[string]any),
	}

	p.registry.Register(prototype)
	return nil
}

// Start starts all workers in the pool.
func (p *WorkerPool) Start() error {
	if p.started {
		return fmt.Errorf("worker pool already started")
	}

	slog.Info("Starting worker pool", "worker_count", p.workerCount, "primary_only", p.primaryOnly)

	p.workers = make([]*Worker, p.workerCount)

	for i := 0; i < p.workerCount; i++ {
		workerID := fmt.Sprintf("worker-%d", i+1)
		p.workers[i] = NewWorker(workerID, p.systemDB, p.registry)
		p.workers[i].SetBatchManager(p.batchManager)
		p.workers[i].SetPrimaryOnlyMode(p.primaryOnly, func() bool {
			return p.cluster.Node().IsPrimary()
		})
		p.workers[i].SetRunningJobKeys(&p.runningJobKeys)
		p.workers[i].SetReservationMutex(&p.reservationMutex)
		p.workers[i].Start()
	}

	p.started = true

	slog.Info("Worker pool started", "worker_count", p.workerCount)

	return nil
}

// Stop gracefully stops all workers in the pool.
func (p *WorkerPool) Stop() {
	if !p.started {
		return
	}

	slog.Info("Stopping worker pool", "worker_count", len(p.workers))

	for _, worker := range p.workers {
		worker.Stop()
	}

	p.workers = nil
	p.started = false

	slog.Info("Worker pool stopped")
}

// IsStarted returns whether the worker pool is currently running.
func (p *WorkerPool) IsStarted() bool {
	return p.started
}

// WorkerCount returns the number of workers in the pool.
func (p *WorkerPool) WorkerCount() int {
	return p.workerCount
}

// NewDispatcher creates a Dispatcher that uses this pool's job registry.
// This ensures dispatched jobs inherit the configuration (retries, timeouts, etc.)
// defined during job registration.
func (p *WorkerPool) NewDispatcher() *Dispatcher {
	dispatcher := NewDispatcher(p.systemDB, p.registry)
	// Update batch manager's dispatcher reference
	p.batchManager.dispatcher = dispatcher

	return dispatcher
}

// NewBatchManager returns a BatchManager that can create batches of jobs.
// The returned BatchManager is connected to this pool's dispatcher.
func (p *WorkerPool) NewBatchManager() *BatchManager {
	return p.batchManager
}

// GetBatchStatus retrieves the current status of a batch by ID.
// This method implements the cluster.WorkerPoolAccessor interface.
func (p *WorkerPool) GetBatchStatus(ctx context.Context, batchID int64) (cluster.BatchProgress, error) {
	progress, err := p.batchManager.GetBatchStatus(ctx, batchID)

	if err != nil {
		return cluster.BatchProgress{}, err
	}

	return cluster.BatchProgress{
		BatchID:       progress.BatchID,
		Name:          progress.Name,
		TotalJobs:     progress.TotalJobs,
		PendingJobs:   progress.PendingJobs,
		CompletedJobs: progress.CompletedJobs,
		FailedJobs:    progress.FailedJobs,
		Progress:      progress.Progress,
		IsFinished:    progress.IsFinished,
		CreatedAt:     progress.CreatedAt,
		FinishedAt:    progress.FinishedAt,
	}, nil
}
