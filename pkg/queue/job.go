package queue

import (
	"fmt"
	"time"
)

// JobHandler is a function that processes job data.
type JobHandler func(data map[string]any) error

// ThrottleFunc is a callback that determines if a job should be throttled.
// It receives the job's data and key, and returns:
// - shouldThrottle: true if the job should be delayed
// - delay: how long to delay the job before retrying
type ThrottleFunc func(data map[string]any, key string) (shouldThrottle bool, delay time.Duration)

// JobTypeConfig defines the configuration for a job type during registration.
type JobTypeConfig struct {
	Name       string // PascalCase job identifier (e.g., "BackupJob")
	QueueName  string
	Retries    int
	RetryAfter time.Duration
	Handler    JobHandler
	Throttle   ThrottleFunc // Optional throttle callback
}

// JobOption is a functional option for configuring job types during registration.
type JobOption func(*JobTypeConfig)

// WithQueue sets which queue this job type should run on.
func WithQueue(queue string) JobOption {
	return func(c *JobTypeConfig) {
		c.QueueName = queue
	}
}

// WithRetries sets the retry behavior for this job type.
func WithRetries(retries int, retryAfter time.Duration) JobOption {
	return func(c *JobTypeConfig) {
		c.Retries = retries
		c.RetryAfter = retryAfter
	}
}

// WithThrottle sets a throttle callback that can conditionally delay job execution.
// The callback receives the job's data and key, and should return whether to throttle
// the job and for how long. If throttled, the job is rescheduled without incrementing
// the retry counter.
func WithThrottle(fn ThrottleFunc) JobOption {
	return func(c *JobTypeConfig) {
		c.Throttle = fn
	}
}

// Job defines the interface that all queued jobs must implement.
// This interface is inspired by Laravel's queue system and provides
// the necessary methods for job processing, retry logic, and identification.
type Job interface {
	// Handle processes the job and returns an error if the job fails.
	// Returning an error will trigger the retry mechanism based on Retries()
	// and RetryAfter() settings.
	Handle() error

	// Name returns the PascalCase identifier for this job (e.g., "BackupJob").
	// This is used to deserialize and instantiate the correct job handler.
	Name() string

	// Key returns a unique identifier for this specific job instance.
	// This can be used for deduplication or tracking individual jobs.
	Key() string

	// QueueName returns the name of the queue this job should be processed on.
	// This allows for job prioritization and separation of concerns.
	QueueName() string

	// Retries returns the maximum number of times this job should be retried
	// if it fails. A value of 0 means no retries, -1 means unlimited retries.
	Retries() int

	// RetryAfter returns the duration to wait before retrying a failed job.
	// This allows for exponential backoff or custom retry strategies.
	RetryAfter() time.Duration

	// Throttle checks if the job should be throttled based on its data and key.
	// Returns shouldThrottle=true and a delay duration if the job should be rescheduled.
	// Returns shouldThrottle=false if the job should proceed normally.
	Throttle() (shouldThrottle bool, delay time.Duration)

	// ToData returns the job's data as a map for serialization to JSON.
	ToData() (map[string]any, error)

	// FromData populates the job from deserialized data.
	FromData(data map[string]any) error

	// NewInstance creates a new instance of the same job type.
	// This is used by the registry to create job instances for deserialization.
	// Returns any to avoid circular import issues, but must return a type that implements Job.
	NewInstance() any
}

// JobConfig provides a fluent API for configuring jobs.
type JobConfig struct {
	name       string // PascalCase job identifier
	key        string
	queueName  string
	retries    int
	retryAfter time.Duration
	handler    func(data map[string]any) error
	data       map[string]any
}

// NewJob creates a new job configuration with the given PascalCase job name.
func NewJob(name string) *JobConfig {
	return &JobConfig{
		name:       name,
		queueName:  "default",
		retries:    3,
		retryAfter: 30 * time.Second,
		data:       make(map[string]any),
	}
}

// Key sets a unique identifier for this job instance.
func (j *JobConfig) Key(key string) *JobConfig {
	j.key = key

	return j
}

// Queue sets the queue name this job should be processed on.
func (j *JobConfig) Queue(queue string) *JobConfig {
	j.queueName = queue

	return j
}

// Retry sets the retry count and delay for failed jobs.
func (j *JobConfig) Retry(retries int, retryAfter time.Duration) *JobConfig {
	j.retries = retries
	j.retryAfter = retryAfter

	return j
}

// Handle sets the handler function that processes the job.
func (j *JobConfig) Handle(handler func(data map[string]any) error) *JobConfig {
	j.handler = handler

	return j
}

// Data sets the data payload for the job.
func (j *JobConfig) Data(data map[string]any) *JobConfig {
	j.data = data

	return j
}

// Build creates the final job instance.
// Returns an error if required fields (handler) are not set.
func (j *JobConfig) Build() (Job, error) {
	if j.handler == nil {
		return nil, fmt.Errorf("job handler is required")
	}

	return &ConfiguredJob{
		name:       j.name,
		key:        j.key,
		queueName:  j.queueName,
		retries:    j.retries,
		retryAfter: j.retryAfter,
		handler:    j.handler,
		data:       j.data,
	}, nil
}

// ConfiguredJob is the implementation created by JobConfig.
type ConfiguredJob struct {
	name         string
	key          string
	queueName    string
	retries      int
	retryAfter   time.Duration
	handler      func(data map[string]any) error
	data         map[string]any
	throttleFunc ThrottleFunc
}

func (j *ConfiguredJob) Handle() error {
	return j.handler(j.data)
}

func (j *ConfiguredJob) Name() string {
	return j.name
}

func (j *ConfiguredJob) Key() string {
	return j.key
}

func (j *ConfiguredJob) QueueName() string {
	return j.queueName
}

func (j *ConfiguredJob) Retries() int {
	return j.retries
}

func (j *ConfiguredJob) RetryAfter() time.Duration {
	return j.retryAfter
}

func (j *ConfiguredJob) Throttle() (shouldThrottle bool, delay time.Duration) {
	if j.throttleFunc == nil {
		return false, 0
	}

	return j.throttleFunc(j.data, j.key)
}

func (j *ConfiguredJob) ToData() (map[string]any, error) {
	return j.data, nil
}

func (j *ConfiguredJob) FromData(data map[string]any) error {
	j.data = data
	return nil
}

func (j *ConfiguredJob) NewInstance() any {
	return &ConfiguredJob{
		name:         j.name,
		queueName:    j.queueName,
		retries:      j.retries,
		retryAfter:   j.retryAfter,
		handler:      j.handler,
		throttleFunc: j.throttleFunc,
		data:         make(map[string]any),
	}
}
