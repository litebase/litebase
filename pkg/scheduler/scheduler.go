package scheduler

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/litebase/litebase/pkg/database"
)

// Scheduler manages and executes scheduled tasks.
type Scheduler struct {
	systemDB     *database.SystemDatabase
	Registry     *TaskRegistry
	ctx          context.Context
	cancel       context.CancelFunc
	wg           sync.WaitGroup
	isPrimary    func() bool
	runningTasks sync.Map // map[string]bool - task name -> is running
}

// NewScheduler creates a new scheduler instance.
func NewScheduler(systemDB *database.SystemDatabase, isPrimary func() bool) *Scheduler {
	ctx, cancel := context.WithCancel(context.Background())

	return &Scheduler{
		systemDB:  systemDB,
		Registry:  NewTaskRegistry(),
		ctx:       ctx,
		cancel:    cancel,
		isPrimary: isPrimary,
	}
}

// RegisterTask registers a new scheduled task.
func (s *Scheduler) RegisterTask(name string, handler TaskHandler, opts ...TaskOption) error {
	task := &RegisteredTask{
		Name:    name,
		Handler: handler,
	}

	// Apply options
	for _, opt := range opts {
		opt(task)
	}

	// Calculate initial next run time
	now := time.Now().UTC()

	nextRun, err := task.CalculateNextRun(now)

	if err != nil {
		return err
	}

	task.SetNextRunAt(nextRun)

	// Register the task
	return s.Registry.Register(task)
}

// Start begins the scheduler supervisor loop.
func (s *Scheduler) Start() {
	s.wg.Go(func() {
		slog.Info("Scheduler started")

		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-s.ctx.Done():
				slog.Info("Scheduler stopped")
				return
			case <-ticker.C:
				s.checkScheduledTasks()
			}
		}
	})
}

// Stop gracefully stops the scheduler.
func (s *Scheduler) Stop() {
	s.cancel()
	s.wg.Wait()
}

// checkScheduledTasks checks if any tasks are due to run.
func (s *Scheduler) checkScheduledTasks() {
	// Only run tasks on primary node
	if s.isPrimary != nil && !s.isPrimary() {
		return
	}

	now := time.Now().UTC()
	tasks := s.Registry.GetAll()

	for _, task := range tasks {
		nextRun := task.NextRunAt()

		// Check if task is due
		if now.Before(nextRun) {
			continue
		}

		// Check for overlap prevention
		if task.WithoutOverlap {
			_, isRunning := s.runningTasks.LoadOrStore(task.Name, true)

			if isRunning {
				slog.Debug("Task already running, skipping", "task", task.Name)
				continue
			}
		}

		// Execute task in a goroutine
		s.wg.Go(func() {
			s.executeTask(task)
		})
	}
}

// executeTask runs a single task.
func (s *Scheduler) executeTask(task *RegisteredTask) {
	// Ensure we remove from running tasks if using overlap prevention
	if task.WithoutOverlap {
		defer s.runningTasks.Delete(task.Name)
	}

	slog.Info("Executing scheduled task", "task", task.Name)

	// Execute the task handler
	err := task.Handler(s.ctx)

	if err != nil {
		slog.Error("Scheduled task failed", "task", task.Name, "error", err)
	} else {
		slog.Info("Scheduled task completed", "task", task.Name)
	}

	// Calculate next run time
	now := time.Now().UTC()
	nextRun, err := task.CalculateNextRun(now)

	if err != nil {
		slog.Error("Failed to calculate next run time", "task", task.Name, "error", err)

		return
	}

	task.SetNextRunAt(nextRun)
	slog.Debug("Task rescheduled", "task", task.Name, "next_run", nextRun)
}
