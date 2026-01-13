package queue

import "time"

// Job defines the interface that all queued jobs must implement.
// This interface is inspired by Laravel's queue system and provides
// the necessary methods for job processing, retry logic, and identification.
type Job interface {
	// Handle processes the job and returns an error if the job fails.
	// Returning an error will trigger the retry mechanism based on Retries()
	// and RetryAfter() settings.
	Handle() error

	// JobType returns the type identifier for this job.
	// This is used to deserialize and instantiate the correct job handler.
	JobType() string

	// Key returns a unique identifier for this specific job instance.
	// This can be used for deduplication or tracking individual jobs.
	Key() string

	// Name returns a human-readable name for the job type.
	// This is used for logging and monitoring purposes.
	Name() string

	// QueueName returns the name of the queue this job should be processed on.
	// This allows for job prioritization and separation of concerns.
	QueueName() string

	// Retries returns the maximum number of times this job should be retried
	// if it fails. A value of 0 means no retries, -1 means unlimited retries.
	Retries() int

	// RetryAfter returns the duration to wait before retrying a failed job.
	// This allows for exponential backoff or custom retry strategies.
	RetryAfter() time.Duration
}