package scheduler

import (
	"context"
	"fmt"
	"sync"
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
	DailyAt     Schedule = "daily_at"  // Requires Time parameter
	Weekly      Schedule = "weekly"    // Runs every week on the same day of week at 00:00 UTC
	WeeklyAt    Schedule = "weekly_at" // Requires Day and Time parameters
	Monthly     Schedule = "monthly"   // Runs on the 1st of every month at 00:00 UTC
)

// RegisteredTask represents a task that has been registered with the scheduler.
type RegisteredTask struct {
	Name           string
	Schedule       Schedule
	Handler        TaskHandler
	Time           string // "14:30" format for DailyAt, WeeklyAt
	Day            string // "Monday", "Tuesday", etc. for WeeklyAt
	WithoutOverlap bool
	nextRunAt      time.Time
	mu             sync.Mutex
}

// NextRunAt returns the next scheduled run time for this task.
func (t *RegisteredTask) NextRunAt() time.Time {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.nextRunAt
}

// SetNextRunAt sets the next scheduled run time for this task.
func (t *RegisteredTask) SetNextRunAt(nextRun time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.nextRunAt = nextRun
}

// CalculateNextRun calculates the next run time based on the task's schedule.
func (t *RegisteredTask) CalculateNextRun(from time.Time) (time.Time, error) {
	now := from.UTC()

	switch t.Schedule {
	case EverySecond:
		return now.Add(1 * time.Second), nil

	case EveryMinute:
		return now.Add(1 * time.Minute), nil

	case Hourly:
		return now.Add(1 * time.Hour), nil

	case Daily:
		// Run at midnight UTC
		next := time.Date(now.Year(), now.Month(), now.Day()+1, 0, 0, 0, 0, time.UTC)
		return next, nil

	case DailyAt:
		// Parse time (e.g., "14:30")
		hour, minute, err := parseTime(t.Time)
		if err != nil {
			return time.Time{}, fmt.Errorf("invalid time format: %w", err)
		}

		// Calculate next occurrence of this time
		next := time.Date(now.Year(), now.Month(), now.Day(), hour, minute, 0, 0, time.UTC)
		if next.Before(now) || next.Equal(now) {
			next = next.AddDate(0, 0, 1)
		}
		return next, nil

	case Weekly:
		// Run at midnight UTC on the same day of week, one week from now
		next := time.Date(now.Year(), now.Month(), now.Day()+7, 0, 0, 0, 0, time.UTC)
		return next, nil

	case WeeklyAt:
		// Parse time and day
		hour, minute, err := parseTime(t.Time)
		if err != nil {
			return time.Time{}, fmt.Errorf("invalid time format: %w", err)
		}

		targetWeekday, err := parseWeekday(t.Day)
		if err != nil {
			return time.Time{}, fmt.Errorf("invalid day: %w", err)
		}

		// Calculate next occurrence of this weekday at this time
		next := time.Date(now.Year(), now.Month(), now.Day(), hour, minute, 0, 0, time.UTC)

		// Adjust to target weekday
		daysUntilTarget := int(targetWeekday - next.Weekday())
		if daysUntilTarget < 0 {
			daysUntilTarget += 7
		}
		next = next.AddDate(0, 0, daysUntilTarget)

		// If we're already past this time today and it's the target weekday, schedule for next week
		if next.Before(now) || next.Equal(now) {
			next = next.AddDate(0, 0, 7)
		}

		return next, nil

	case Monthly:
		// Run at midnight UTC on the 1st of next month
		next := time.Date(now.Year(), now.Month()+1, 1, 0, 0, 0, 0, time.UTC)
		return next, nil

	default:
		return time.Time{}, fmt.Errorf("unknown schedule type: %s", t.Schedule)
	}
}

// parseTime parses a time string in "HH:MM" format.
func parseTime(timeStr string) (hour, minute int, err error) {
	_, err = fmt.Sscanf(timeStr, "%d:%d", &hour, &minute)
	if err != nil {
		return 0, 0, err
	}

	if hour < 0 || hour > 23 {
		return 0, 0, fmt.Errorf("hour must be between 0 and 23")
	}

	if minute < 0 || minute > 59 {
		return 0, 0, fmt.Errorf("minute must be between 0 and 59")
	}

	return hour, minute, nil
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

// TaskOption is a functional option for configuring a task.
type TaskOption func(*RegisteredTask)

// WithSchedule sets the schedule for a task.
func WithSchedule(schedule Schedule) TaskOption {
	return func(t *RegisteredTask) {
		t.Schedule = schedule
	}
}

// WithTime sets the time for DailyAt and WeeklyAt schedules.
func WithTime(timeStr string) TaskOption {
	return func(t *RegisteredTask) {
		t.Time = timeStr
	}
}

// WithDay sets the day for WeeklyAt schedules.
func WithDay(day string) TaskOption {
	return func(t *RegisteredTask) {
		t.Day = day
	}
}

// WithoutOverlap prevents a task from running if a previous execution is still running.
func WithoutOverlap() TaskOption {
	return func(t *RegisteredTask) {
		t.WithoutOverlap = true
	}
}
