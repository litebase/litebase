package scheduler

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
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
	jobs      map[string]gocron.Job // Store job references for gap analysis
	jobsMutex sync.RWMutex          // Protect concurrent access to jobs map
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
		jobs:      make(map[string]gocron.Job),
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
	job, err := s.scheduler.NewJob(
		jobDef,
		gocron.NewTask(wrappedHandler),
		jobOpts...,
	)

	if err != nil {
		return fmt.Errorf("failed to create job: %w", err)
	}

	// Store job reference for gap analysis
	s.jobsMutex.Lock()
	s.jobs[name] = job
	s.jobsMutex.Unlock()

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

// MissedExecution represents a task execution that was missed during downtime.
type MissedExecution struct {
	TaskName    string
	Task        *RegisteredTask
	ScheduledAt time.Time
	MissedBy    time.Duration
}

// AnalyzeGaps detects tasks that were missed during cluster downtime.
// Returns a list of missed critical task executions that should be caught up.
func (s *Scheduler) AnalyzeGaps() ([]MissedExecution, error) {
	// 1. Get last shutdown timestamp from metadata table
	db, err := s.systemDB.DB()

	if err != nil {
		return nil, fmt.Errorf("failed to get system database: %w", err)
	}

	var lastShutdown string
	err = db.QueryRow("SELECT value FROM metadata WHERE key = ?", "primary_node_stopped_at").Scan(&lastShutdown)

	if err != nil {
		// No previous shutdown recorded - this is first startup or untracked shutdown
		slog.Info("No previous shutdown timestamp found - skipping gap analysis")
		return nil, nil
	}

	lastShutdownTime, err := time.Parse(time.RFC3339, lastShutdown)

	if err != nil {
		return nil, fmt.Errorf("failed to parse shutdown timestamp: %w", err)
	}

	now := time.Now().UTC()
	downtimeWindow := now.Sub(lastShutdownTime)

	slog.Info("Analyzing gaps in scheduled tasks",
		"last_shutdown", lastShutdownTime,
		"downtime_duration", downtimeWindow)

	// 2. For each registered critical task, check if it should have run
	missed := []MissedExecution{}

	s.jobsMutex.RLock()
	defer s.jobsMutex.RUnlock()

	for name, job := range s.jobs {
		task, err := s.Registry.Get(name)

		if err != nil {
			continue
		}

		// Only analyze critical tasks
		if !task.IsCritical {
			continue
		}

		// Get the next scheduled run time from gocron
		nextRun, err := job.NextRun()

		if err != nil {
			slog.Warn("Failed to get next run time for task", "task", name, "error", err)
			continue
		}

		// Calculate when this task should have run during downtime
		// We need to work backwards from nextRun to find the last scheduled time before shutdown
		scheduledTime := s.calculateLastScheduledRunBeforeShutdown(task, lastShutdownTime, nextRun)

		// If the scheduled time was during downtime and before now, it was missed
		if scheduledTime.After(lastShutdownTime) && scheduledTime.Before(now) {
			missed = append(missed, MissedExecution{
				TaskName:    name,
				Task:        task,
				ScheduledAt: scheduledTime,
				MissedBy:    now.Sub(scheduledTime),
			})
		}
	}

	if len(missed) > 0 {
		slog.Warn("Detected missed critical task executions",
			"count", len(missed),
			"downtime_duration", downtimeWindow)
	} else {
		slog.Info("No missed critical task executions detected")
	}

	return missed, nil
}

// calculateLastScheduledRunBeforeShutdown determines when a task should have last run
// before the shutdown timestamp by working backwards from the next scheduled run.
func (s *Scheduler) calculateLastScheduledRunBeforeShutdown(task *RegisteredTask, shutdown, nextRun time.Time) time.Time {
	now := time.Now().UTC()

	// For schedules with specific times, we need to find the last occurrence
	// If nextRun is in the future, the last run should have been one period ago

	switch task.Schedule {
	case EverySecond:
		// If nextRun is in future, last run was 1 second before nextRun
		if nextRun.After(now) {
			return nextRun.Add(-1 * time.Second)
		}

		return now.Add(-1 * time.Second)
	case EveryMinute:
		if nextRun.After(now) {
			return nextRun.Add(-1 * time.Minute)
		}

		return now.Add(-1 * time.Minute)
	case Hourly:
		if nextRun.After(now) {
			return nextRun.Add(-1 * time.Hour)
		}

		return now.Add(-1 * time.Hour)
	case Daily:
		// Daily tasks run at a specific time each day
		// If nextRun is in the future (e.g., tomorrow at 3am),
		// the last scheduled run was 24 hours before that
		if nextRun.After(now) {
			return nextRun.Add(-24 * time.Hour)
		}

		// If nextRun is in the past (missed today's run), use yesterday
		return now.Add(-24 * time.Hour).Truncate(24 * time.Hour).Add(
			time.Duration(nextRun.Hour())*time.Hour +
				time.Duration(nextRun.Minute())*time.Minute)
	case Weekly:
		if nextRun.After(now) {
			return nextRun.Add(-7 * 24 * time.Hour)
		}

		return now.Add(-7 * 24 * time.Hour)
	case Monthly:
		// Go back one month
		if nextRun.After(now) {
			return nextRun.AddDate(0, -1, 0)
		}

		return now.AddDate(0, -1, 0)
	case Cron:
		// TODO: Investigaate...

		// For cron expressions, we can't easily calculate backwards
		// Use a heuristic: if there was downtime, assume at least one execution was missed
		// Return a time just after shutdown to indicate a missed execution
		return shutdown.Add(1 * time.Minute)
	default:
		return shutdown
	}
}

// ExecuteMissedTasks executes critical tasks that were missed during downtime.
// Each missed task is executed once, regardless of how many times it should have run.
// This "collapses the window" to prevent job storms.
func (s *Scheduler) ExecuteMissedTasks(missed []MissedExecution) {
	if len(missed) == 0 {
		return
	}

	slog.Info("Executing catch-up for missed critical tasks", "count", len(missed))

	for _, me := range missed {
		slog.Info("Catching up missed task",
			"task", me.TaskName,
			"scheduled_at", me.ScheduledAt,
			"missed_by", me.MissedBy)

		// Execute the task immediately
		// Use the main scheduler context for cancellation
		go func(task *RegisteredTask) {
			err := task.Handler(s.ctx)

			if err != nil {
				slog.Error("Catch-up task execution failed",
					"task", task.Name,
					"error", err)
			} else {
				slog.Info("Catch-up task execution completed successfully",
					"task", task.Name)
			}
		}(me.Task)
	}
}
