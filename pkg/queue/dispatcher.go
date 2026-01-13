package queue

import (
	"fmt"
	"time"

	"github.com/litebase/litebase/pkg/database"
)

// Dispatcher is responsible for dispatching jobs to the queue.
// It handles job persistence, deduplication, and scheduling.
type Dispatcher struct {
	systemDB *database.SystemDatabase
}

// NewDispatcher creates a new Dispatcher instance with the system database.
func NewDispatcher(systemDB *database.SystemDatabase) *Dispatcher {
	return &Dispatcher{
		systemDB: systemDB,
	}
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

	now := time.Now().UTC()
	availableAt := now.Add(delay)

	result, err := db.Exec(
		`INSERT INTO queued_jobs 
		(queue_name, job_type, key, status, attempts, max_attempts, created_at, updated_at, available_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		job.QueueName(),
		job.JobType(),
		job.Key(),
		JobStatusPending,
		0,
		job.Retries(),
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
