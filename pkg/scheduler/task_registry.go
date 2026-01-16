package scheduler

import (
	"fmt"
	"sync"
)

// TaskRegistry maintains a registry of scheduled tasks.
type TaskRegistry struct {
	tasks map[string]*RegisteredTask
	mu    sync.RWMutex
}

// NewTaskRegistry creates a new task registry.
func NewTaskRegistry() *TaskRegistry {
	return &TaskRegistry{
		tasks: make(map[string]*RegisteredTask),
	}
}

// Register adds a task to the registry.
func (r *TaskRegistry) Register(task *RegisteredTask) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.tasks[task.Name]; exists {
		return fmt.Errorf("task %s is already registered", task.Name)
	}

	r.tasks[task.Name] = task
	return nil
}

// GetAll returns all registered tasks.
func (r *TaskRegistry) GetAll() []*RegisteredTask {
	r.mu.RLock()
	defer r.mu.RUnlock()

	tasks := make([]*RegisteredTask, 0, len(r.tasks))
	for _, task := range r.tasks {
		tasks = append(tasks, task)
	}

	return tasks
}

// Get returns a task by name.
func (r *TaskRegistry) Get(name string) (*RegisteredTask, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	task, exists := r.tasks[name]
	if !exists {
		return nil, fmt.Errorf("task %s not found", name)
	}

	return task, nil
}
