package scheduler

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/go-co-op/gocron/v2"
	"github.com/litebase/litebase/pkg/database"
)

// Scheduler manages and executes scheduled tasks.
type Scheduler struct {
	systemDB  *database.SystemDatabase
	Registry  *TaskRegistry
	scheduler gocron.Scheduler
	isPrimary func() bool
	ctx       context.Context
	cancel    context.CancelFunc
}

// NewScheduler creates a new scheduler instance.
func NewScheduler(systemDB *database.SystemDatabase, isPrimary func() bool) *Scheduler {
	ctx, cancel := context.WithCancel(context.Background())

	s, err := gocron.NewScheduler()

	if err != nil {
		panic(err) // This should never happen with default config
	}

	return &Scheduler{
		systemDB:  systemDB,
		Registry:  NewTaskRegistry(),
		scheduler: s,
		isPrimary: isPrimary,
		ctx:       ctx,
		cancel:    cancel,
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

	// Register with registry
	if err := s.Registry.Register(task); err != nil {
		return err
	}

	// Convert schedule to gocron job definition
	jobDef, err := s.scheduleToJobDef(task)

	if err != nil {
		return fmt.Errorf("failed to create job definition: %w", err)
	}

	// Wrap handler to only run on primary
	wrappedHandler := func() {
		// Only execute on primary node
		if s.isPrimary != nil && !s.isPrimary() {
			return
		}

		s.executeTask(task)
	}

	// Configure job options
	jobOpts := []gocron.JobOption{}

	if task.WithoutOverlap {
		jobOpts = append(jobOpts, gocron.WithSingletonMode(gocron.LimitModeReschedule))
	}

	// Create the job with gocron
	_, err = s.scheduler.NewJob(
		jobDef,
		gocron.NewTask(wrappedHandler),
		jobOpts...,
	)

	if err != nil {
		return fmt.Errorf("failed to create job: %w", err)
	}

	slog.Info("Registered scheduled task", "task", name, "schedule", task.Schedule)

	return nil
}

// scheduleToJobDef converts our schedule types to gocron job definitions.
func (s *Scheduler) scheduleToJobDef(task *RegisteredTask) (gocron.JobDefinition, error) {
	// If the task has a cron expression, use it directly
	if task.CronExpr != "" {
		return gocron.CronJob(task.CronExpr, false), nil
	}

	switch task.Schedule {
	case EverySecond:
		return gocron.DurationJob(1 * time.Second), nil
	case EveryMinute:
		return gocron.DurationJob(1 * time.Minute), nil
	case Hourly:
		return gocron.DurationJob(1 * time.Hour), nil
	case Daily:
		if task.Time != "" {
			t, err := parseTime(task.Time)

			if err != nil {
				return nil, err
			}

			return gocron.DailyJob(1, gocron.NewAtTimes(
				gocron.NewAtTime(uint(t.Hour()), uint(t.Minute()), 0),
			)), nil
		}

		return gocron.DailyJob(1, gocron.NewAtTimes(
			gocron.NewAtTime(0, 0, 0),
		)), nil
	case Weekly:
		weekday := time.Monday // Default

		if task.Weekday != "" {
			wd, err := parseWeekday(task.Weekday)

			if err == nil {
				weekday = wd
			}
		}

		if task.Time != "" {
			t, err := parseTime(task.Time)

			if err != nil {
				return nil, err
			}

			return gocron.WeeklyJob(1, gocron.NewWeekdays(weekday), gocron.NewAtTimes(
				gocron.NewAtTime(uint(t.Hour()), uint(t.Minute()), 0),
			)), nil
		}

		return gocron.WeeklyJob(1, gocron.NewWeekdays(weekday), gocron.NewAtTimes(
			gocron.NewAtTime(0, 0, 0),
		)), nil
	case Monthly:
		day := 1 // Default to first day of month

		if task.Day != 0 {
			day = task.Day
		}

		if task.Time != "" {
			t, err := parseTime(task.Time)

			if err != nil {
				return nil, err
			}

			return gocron.MonthlyJob(1, gocron.NewDaysOfTheMonth(day), gocron.NewAtTimes(
				gocron.NewAtTime(uint(t.Hour()), uint(t.Minute()), 0),
			)), nil
		}

		return gocron.MonthlyJob(1, gocron.NewDaysOfTheMonth(day), gocron.NewAtTimes(
			gocron.NewAtTime(0, 0, 0),
		)), nil
	default:
		return nil, fmt.Errorf("unsupported schedule type: %s", task.Schedule)
	}
}

// Start begins the scheduler.
func (s *Scheduler) Start() {
	s.scheduler.Start()
	slog.Info("Scheduler started")
}

// Stop gracefully stops the scheduler.
func (s *Scheduler) Stop() error {
	s.cancel()
	return s.scheduler.Shutdown()
}

// executeTask runs a single task.
func (s *Scheduler) executeTask(task *RegisteredTask) {
	slog.Info("Executing scheduled task", "task", task.Name)

	// Execute the task handler
	err := task.Handler(s.ctx)

	if err != nil {
		slog.Error("Scheduled task failed", "task", task.Name, "error", err)
	} else {
		slog.Info("Scheduled task completed", "task", task.Name)
	}
}
