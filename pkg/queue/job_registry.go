package queue

import (
	"fmt"
	"sync"
)

// JobRegistry maps job types to their factory functions.
type JobRegistry struct {
	factories map[string]JobFactory
	mu        sync.RWMutex
}

// JobFactory is a function that creates a job instance given a key.
type JobFactory func(key string) (Job, error)

// NewJobRegistry creates a new job registry.
func NewJobRegistry() *JobRegistry {
	return &JobRegistry{
		factories: make(map[string]JobFactory),
	}
}

// Register adds a job factory to the registry.
func (r *JobRegistry) Register(jobType string, factory JobFactory) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.factories[jobType] = factory
}

// Get retrieves a job instance for the given type and key.
func (r *JobRegistry) Get(jobType string, key string) (Job, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	factory, ok := r.factories[jobType]

	if !ok {
		return nil, fmt.Errorf("job type %s not registered", jobType)
	}

	return factory(key)
}
