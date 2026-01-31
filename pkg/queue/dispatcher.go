package queue

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/litebase/litebase/pkg/database"
)

// dispatchConfig holds configuration for dispatching a job.
type dispatchConfig struct {
	key    string
	delay  time.Duration
	unique bool
}

// DispatchOption is a functional option for configuring job dispatch.
type DispatchOption func(*dispatchConfig)

// WithKey sets a unique key for the job instance.
func WithKey(key string) DispatchOption {
	return func(c *dispatchConfig) {
		c.key = key
	}
}

// WithDelay sets a delay before the job becomes available for processing.
func WithDelay(delay time.Duration) DispatchOption {
	return func(c *dispatchConfig) {
		c.delay = delay
	}
}

// Unique marks the job as unique (only one instance with the same key can be pending).
func Unique() DispatchOption {
	return func(c *dispatchConfig) {
		c.unique = true
	}
}

// Dispatcher is responsible for dispatching jobs to the queue.
// It handles job persistence, deduplication, and scheduling.
type Dispatcher struct {
	systemDB   *database.SystemDatabase
	registry   *JobRegistry
	workerPool WorkerPoolTriggerer // Optional: for waking workers immediately
}

// WorkerPoolTriggerer interface allows dispatcher to wake workers without circular dependency
type WorkerPoolTriggerer interface {
	TriggerWorkers()
}

// NewDispatcher creates a new Dispatcher instance with the system database and job registry.
func NewDispatcher(systemDB *database.SystemDatabase, registry *JobRegistry) *Dispatcher {
	return &Dispatcher{
		systemDB: systemDB,
		registry: registry,
	}
}

// SetWorkerPool sets the worker pool reference for triggering workers.
func (d *Dispatcher) SetWorkerPool(pool WorkerPoolTriggerer) {
	d.workerPool = pool
}

// DispatchJob dispatches a job by type with the given data.
// This is the preferred way to dispatch jobs as it's more concise.
func (d *Dispatcher) DispatchJob(name string, data map[string]any, opts ...DispatchOption) (int64, error) {
	config := &dispatchConfig{
		key:    "",
		delay:  0,
		unique: false,
	}

	for _, opt := range opts {
		opt(config)
	}

	// Get the job prototype from the registry and create a new instance
	job, err := d.registry.Get(name, data)
	if err != nil {
		return 0, fmt.Errorf("failed to get job from registry: %w", err)
	}

	// If a custom key was provided via WithKey(), apply it to the job
	if config.key != "" {
		if configuredJob, ok := job.(*ConfiguredJob); ok {
			configuredJob.SetKey(config.key)
		}
	}

	if config.unique {
		id, _, err := d.DispatchUnique(job)
		return id, err
	}

	if config.delay > 0 {
		return d.DispatchWithDelay(job, config.delay)
	}

	return d.Dispatch(job)
}

// Dispatch adds a new job to the queue.
// Returns the ID of the queued job or an error if the dispatch fails.
func (d *Dispatcher) Dispatch(job Job) (int64, error) {
	return d.DispatchWithDelay(job, 0)
}

// DispatchWithDelay adds a new job to the queue with a specified delay.
// The job will not be available for processing until the delay has elapsed.
func (d *Dispatcher) DispatchWithDelay(job Job, delay time.Duration) (int64, error) {
	db, err := d.systemDB.DB()

	if err != nil {
		return 0, fmt.Errorf("failed to get system database: %w", err)
	}

	// Serialize job data to JSON
	jobData, err := job.ToData()

	if err != nil {
		return 0, fmt.Errorf("failed to serialize job data: %w", err)
	}

	dataJSON, err := json.Marshal(jobData)

	if err != nil {
		return 0, fmt.Errorf("failed to marshal job data to JSON: %w", err)
	}

	now := time.Now().UTC()
	availableAt := now.Add(delay)

	result, err := db.Exec(
		`INSERT INTO queued_jobs 
		(queue_name, name, key, status, attempts, max_attempts, data, created_at, updated_at, available_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		job.QueueName(),
		job.Name(),
		job.Key(),
		JobStatusPending,
		0,
		job.Retries(),
		string(dataJSON),
		now.Format(time.RFC3339),
		now.Format(time.RFC3339),
		availableAt.Format(time.RFC3339),
	)

	if err != nil {
		return 0, fmt.Errorf("failed to dispatch job: %w", err)
	}

	id, err := result.LastInsertId()

	if err != nil {
		return 0, fmt.Errorf("failed to get inserted job ID: %w", err)
	}

	// Wake workers immediately if no delay (instant jobs)
	if delay == 0 && d.workerPool != nil {
		d.workerPool.TriggerWorkers()
	}

	return id, nil
}

// DispatchUnique adds a job to the queue only if a job with the same key doesn't already exist
// in pending or reserved status. This allows jobs with the same key to exist if they are completed or failed.
// Returns the ID of the existing or newly created job, and a boolean indicating if it was newly created.
func (d *Dispatcher) DispatchUnique(job Job) (int64, bool, error) {
	return d.DispatchUniqueWithDelay(job, 0)
}

// DispatchUniqueWithDelay adds a job to the queue with a delay only if an unprocessed job
// with the same key doesn't already exist (pending or reserved status).
// Returns the ID of the job, whether it was newly created, and any error.
func (d *Dispatcher) DispatchUniqueWithDelay(job Job, delay time.Duration) (int64, bool, error) {
	db, err := d.systemDB.DB()

	if err != nil {
		return 0, false, fmt.Errorf("failed to get system database: %w", err)
	}

	// Check if an unprocessed job with this key already exists (pending or reserved)
	var existingID int64

	err = db.QueryRow(
		`SELECT id FROM queued_jobs WHERE key = ? AND status IN (?, ?)`,
		job.Key(),
		JobStatusPending,
		JobStatusReserved,
	).Scan(&existingID)

	if err == nil {
		// Job already exists
		return existingID, false, nil
	}

	if err.Error() != "sql: no rows in result set" {
		// Unexpected error
		return 0, false, fmt.Errorf("failed to check for existing job: %w", err)
	}

	// No existing job, dispatch a new one
	id, err := d.DispatchWithDelay(job, delay)

	if err != nil {
		return 0, false, err
	}

	return id, true, nil
}
