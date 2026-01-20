package queue

import "time"

// BatchStatus represents the status of a batched job within a batch.
type BatchStatus string

const (
	BatchStatusPending   BatchStatus = "pending"   // Job is waiting to be processed
	BatchStatusProcessing BatchStatus = "processing" // Job is currently being processed
	BatchStatusCompleted BatchStatus = "completed" // Job has been successfully processed
	BatchStatusFailed    BatchStatus = "failed"    // Job has failed
)

// JobBatch represents a batch of jobs tracked together.
type JobBatch struct {
	ID          int64      `db:"id"`
	Name        string     `db:"name"`
	TotalJobs   int        `db:"total_jobs"`
	PendingJobs int        `db:"pending_jobs"`
	FailedJobs  int        `db:"failed_jobs"`
	CreatedAt   time.Time  `db:"created_at"`
	FinishedAt  *time.Time `db:"finished_at"`
}

// BatchedJob represents a job that is part of a batch.
type BatchedJob struct {
	ID        int64       `db:"id"`
	BatchID   int64       `db:"batch_id"`
	QueueID   int64       `db:"queue_id"`
	Status    BatchStatus `db:"status"`
	Progress  int         `db:"progress"` // 0-100
	CreatedAt time.Time   `db:"created_at"`
	UpdatedAt time.Time   `db:"updated_at"`
}

// BatchProgress represents the current progress of a batch.
type BatchProgress struct {
	BatchID      int64
	Name         string
	TotalJobs    int
	PendingJobs  int
	CompletedJobs int
	FailedJobs   int
	Progress     int // Percentage 0-100
	IsFinished   bool
	CreatedAt    time.Time
	FinishedAt   *time.Time
}
