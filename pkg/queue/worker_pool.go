package queue

import (
	"fmt"
	"log/slog"
	"runtime"
	"time"

	"github.com/litebase/litebase/pkg/cluster"
	"github.com/litebase/litebase/pkg/database"
)

// WorkerPool manages a pool of workers that process jobs from the queue.
type WorkerPool struct {
	workers     []*Worker
	systemDB    *database.SystemDatabase
	cluster     *cluster.Cluster
	registry    *JobRegistry
	workerCount int
	started     bool
	primaryOnly bool
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

	return &WorkerPool{
		systemDB:    systemDB,
		cluster:     cluster,
		registry:    NewJobRegistry(),
		workerCount: workerCount,
		primaryOnly: config.PrimaryOnly,
	}
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
		name:         config.Name,
		queueName:    config.QueueName,
		retries:      config.Retries,
		retryAfter:   config.RetryAfter,
		handler:      config.Handler,
		throttleFunc: config.Throttle,
		data:         make(map[string]any),
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
		p.workers[i].SetPrimaryOnlyMode(p.primaryOnly, func() bool {
			return p.cluster.Node().IsPrimary()
		})
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
