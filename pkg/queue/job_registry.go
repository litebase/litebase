package queue

import (
	"fmt"
	"sync"
)

// JobRegistry maps job types to their prototype instances.
type JobRegistry struct {
	prototypes map[string]Job
	mu         sync.RWMutex
}

// NewJobRegistry creates a new job registry.
func NewJobRegistry() *JobRegistry {
	return &JobRegistry{
		prototypes: make(map[string]Job),
	}
}

// Register adds a job prototype to the registry.
// The job's Name() method is used to determine the type identifier.
func (r *JobRegistry) Register(job Job) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.prototypes[job.Name()] = job
}

// Get retrieves a new job instance for the given type and hydrates it with data.
func (r *JobRegistry) Get(jobType string, data map[string]any) (Job, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	prototype, ok := r.prototypes[jobType]

	if !ok {
		return nil, fmt.Errorf("job type %s not registered", jobType)
	}

	// Create a new instance using the prototype
	// NewInstance() may return a concrete type that implements Job interface
	newInstance := prototype.NewInstance()

	// Assert that it implements Job
	job, ok := newInstance.(Job)

	if !ok {
		return nil, fmt.Errorf("NewInstance for job type %s did not return a valid Job", jobType)
	}

	// Hydrate the job with data if provided
	if data != nil {
		if err := job.FromData(data); err != nil {
			return nil, fmt.Errorf("failed to hydrate job from data: %w", err)
		}
	}

	return job, nil
}
