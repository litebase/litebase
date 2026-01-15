package queue

import "time"

// JobStatus represents the current state of a queued job.
type JobStatus string

const (
	JobStatusPending   JobStatus = "pending"   // Job is waiting to be processed
	JobStatusReserved  JobStatus = "reserved"  // Job has been picked up by a worker
	JobStatusCompleted JobStatus = "completed" // Job has been successfully processed
	JobStatusFailed    JobStatus = "failed"    // Job has failed and will not be retried
)

// QueuedJob represents a job stored in the system database queue.
// This struct maps to the queued_jobs table and contains all metadata
// needed to process, retry, and track jobs.
type QueuedJob struct {
	ID          int64      `db:"id"`
	QueueName   string     `db:"queue_name"`
	Name        string     `db:"name"`
	Key         string     `db:"key"`
	Status      JobStatus  `db:"status"`
	Attempts    int        `db:"attempts"`
	MaxAttempts int        `db:"max_attempts"`
	CreatedAt   time.Time  `db:"created_at"`
	UpdatedAt   time.Time  `db:"updated_at"`
	AvailableAt time.Time  `db:"available_at"`
	ReservedAt  *time.Time `db:"reserved_at"`
	ReservedBy  *string    `db:"reserved_by"`
	CompletedAt *time.Time `db:"completed_at"`
	ErrorLog    *string    `db:"error_log"`
}
