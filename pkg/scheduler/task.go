package scheduler

import (
	"context"
	"fmt"
	"time"
)

// TaskHandler is a function that executes a scheduled task.
type TaskHandler func(ctx context.Context) error

// Schedule represents the frequency at which a task should run.
type Schedule string

const (
	EverySecond Schedule = "every_second"
	EveryMinute Schedule = "every_minute"
	Hourly      Schedule = "hourly"
	Daily       Schedule = "daily"
	Weekly      Schedule = "weekly"
	Monthly     Schedule = "monthly"
	Cron        Schedule = "cron" // Uses CronExpr field
)

// RegisteredTask represents a task that has been registered with the scheduler.
type RegisteredTask struct {
	Name           string
	Handler        TaskHandler
	Schedule       Schedule
	Time           string // "14:30" format for Daily, Weekly
	Weekday        string // "Monday", "Tuesday", etc. for Weekly
	Day            int    // Day of month (1-31) for Monthly
	CronExpr       string // Cron expression when Schedule is Cron
	WithoutOverlap bool
	IsCritical     bool // If true, task will be caught up after downtime
}

// TaskOption is a functional option for configuring a task.
type TaskOption func(*RegisteredTask)

// WithSchedule sets the schedule for a task.
func WithSchedule(schedule Schedule) TaskOption {
	return func(t *RegisteredTask) {
		t.Schedule = schedule
	}
}

// WithTime sets the time for Daily and Weekly schedules (HH:MM format).
func WithTime(timeStr string) TaskOption {
	return func(t *RegisteredTask) {
		t.Time = timeStr
	}
}

// WithWeekday sets the day of week for Weekly schedules.
func WithWeekday(day string) TaskOption {
	return func(t *RegisteredTask) {
		t.Weekday = day
	}
}

// WithDay sets the day of month for Monthly schedules.
func WithDay(day int) TaskOption {
	return func(t *RegisteredTask) {
		t.Day = day
	}
}

// WithCron sets a cron expression for flexible scheduling.
// Examples:
//   - "*/5 * * * *" - every 5 minutes
//   - "0 2,14 * * *" - twice daily at 2am and 2pm
//   - "0 */6 * * *" - every 6 hours
//   - "0 0 * * 0" - weekly on Sunday at midnight
func WithCron(cronExpr string) TaskOption {
	return func(t *RegisteredTask) {
		t.Schedule = Cron
		t.CronExpr = cronExpr
	}
}

// WithoutOverlap prevents a task from running if a previous execution is still running.
func WithoutOverlap() TaskOption {
	return func(t *RegisteredTask) {
		t.WithoutOverlap = true
	}
}

// WithCritical marks a task as critical for catch-up execution.
// Critical tasks that were missed during downtime will be executed once on startup.
// Examples: backups, cleanups, data maintenance tasks.
func WithCritical() TaskOption {
	return func(t *RegisteredTask) {
		t.IsCritical = true
	}
}

// parseTime parses a time string in "HH:MM" format and returns a time.Time for today at that time.
func parseTime(timeStr string) (time.Time, error) {
	var hour, minute int
	_, err := fmt.Sscanf(timeStr, "%d:%d", &hour, &minute)
	if err != nil {
		return time.Time{}, err
	}

	if hour < 0 || hour > 23 {
		return time.Time{}, fmt.Errorf("hour must be between 0 and 23")
	}

	if minute < 0 || minute > 59 {
		return time.Time{}, fmt.Errorf("minute must be between 0 and 59")
	}

	now := time.Now().UTC()
	return time.Date(now.Year(), now.Month(), now.Day(), hour, minute, 0, 0, time.UTC), nil
}

// parseWeekday parses a weekday string (e.g., "Monday") into time.Weekday.
func parseWeekday(day string) (time.Weekday, error) {
	switch day {
	case "Sunday":
		return time.Sunday, nil
	case "Monday":
		return time.Monday, nil
	case "Tuesday":
		return time.Tuesday, nil
	case "Wednesday":
		return time.Wednesday, nil
	case "Thursday":
		return time.Thursday, nil
	case "Friday":
		return time.Friday, nil
	case "Saturday":
		return time.Saturday, nil
	default:
		return 0, fmt.Errorf("invalid weekday: %s", day)
	}
}
