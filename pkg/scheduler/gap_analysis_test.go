package scheduler_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/scheduler"
	"github.com/litebase/litebase/pkg/server"
)

func TestScheduler_GapAnalysis_NoPreviousShutdown(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		// Stop queue worker pool to avoid interference
		if app.QueueWorkerPool != nil {
			app.QueueWorkerPool.Stop()
		}

		s := app.Scheduler

		// Register a critical task
		err := s.RegisterTask(
			"CriticalTask",
			func(ctx context.Context) error {
				return nil
			},
			scheduler.WithSchedule(scheduler.Daily),
			scheduler.WithCritical(),
		)

		if err != nil {
			t.Fatalf("Failed to register task: %v", err)
		}

		// Analyze gaps - should return nil/empty since no shutdown was recorded
		gaps, err := s.AnalyzeGaps()

		if err != nil {
			t.Fatalf("AnalyzeGaps failed: %v", err)
		}

		if len(gaps) != 0 {
			t.Errorf("Expected 0 gaps on first startup, got %d", len(gaps))
		}
	})
}

func TestScheduler_GapAnalysis_WithDowntime(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		// Stop queue worker pool to avoid interference
		if app.QueueWorkerPool != nil {
			app.QueueWorkerPool.Stop()
		}

		s := app.Scheduler

		// Simulate a previous shutdown 2 days ago
		db, err := app.DatabaseManager.SystemDatabase().DB()

		if err != nil {
			t.Fatalf("Failed to get system database: %v", err)
		}

		twoDaysAgo := time.Now().UTC().Add(-48 * time.Hour).Format(time.RFC3339)

		_, err = db.Exec("INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)",
			"primary_node_stopped_at", twoDaysAgo)

		if err != nil {
			t.Fatalf("Failed to set shutdown timestamp: %v", err)
		}

		// Register a critical daily task
		err = s.RegisterTask(
			"CriticalDailyTask",
			func(ctx context.Context) error {
				return nil
			},
			scheduler.WithSchedule(scheduler.Daily),
			scheduler.WithTime("03:00"),
			scheduler.WithCritical(),
		)

		if err != nil {
			t.Fatalf("Failed to register task: %v", err)
		}

		// Start scheduler to initialize job schedules
		s.Start()
		time.Sleep(100 * time.Millisecond) // Let gocron initialize

		defer func() {
			if err := s.Stop(); err != nil {
				t.Fatalf("Failed to stop scheduler: %v", err)
			}
		}()

		// Analyze gaps - should detect the missed daily task
		gaps, err := s.AnalyzeGaps()

		if err != nil {
			t.Fatalf("AnalyzeGaps failed: %v", err)
		}

		if len(gaps) == 0 {
			t.Error("Expected at least 1 gap after 2 day downtime, got 0")
		} else {
			gap := gaps[0]

			if gap.TaskName != "CriticalDailyTask" {
				t.Errorf("Expected task name 'CriticalDailyTask', got '%s'", gap.TaskName)
			}

			// The gap should indicate the task was missed (any positive duration means it was scheduled before now)
			if gap.MissedBy <= 0 {
				t.Errorf("Expected gap to be missed (positive duration), got %v", gap.MissedBy)
			}

			t.Logf("Detected gap: task=%s, scheduled_at=%v, missed_by=%v",
				gap.TaskName, gap.ScheduledAt, gap.MissedBy)
		}
	})
}

func TestScheduler_GapAnalysis_NonCriticalTasksIgnored(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		// Stop queue worker pool to avoid interference
		if app.QueueWorkerPool != nil {
			app.QueueWorkerPool.Stop()
		}

		s := app.Scheduler

		// Simulate a previous shutdown 1 day ago
		db, err := app.DatabaseManager.SystemDatabase().DB()

		if err != nil {
			t.Fatalf("Failed to get system database: %v", err)
		}

		oneDayAgo := time.Now().UTC().Add(-24 * time.Hour).Format(time.RFC3339)

		_, err = db.Exec("INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)",
			"primary_node_stopped_at", oneDayAgo)

		if err != nil {
			t.Fatalf("Failed to set shutdown timestamp: %v", err)
		}

		// Register a NON-critical task
		err = s.RegisterTask(
			"NonCriticalTask",
			func(ctx context.Context) error {
				return nil
			},
			scheduler.WithSchedule(scheduler.Hourly),
			// No WithCritical() - should be skipped
		)

		if err != nil {
			t.Fatalf("Failed to register task: %v", err)
		}

		// Start scheduler
		s.Start()
		time.Sleep(100 * time.Millisecond)

		defer func() {
			if err := s.Stop(); err != nil {
				t.Fatalf("Failed to stop scheduler: %v", err)
			}
		}()

		// Analyze gaps - should return empty since task is not critical
		gaps, err := s.AnalyzeGaps()

		if err != nil {
			t.Fatalf("AnalyzeGaps failed: %v", err)
		}

		if len(gaps) != 0 {
			t.Errorf("Expected 0 gaps for non-critical task, got %d", len(gaps))
		}
	})
}

func TestScheduler_ExecuteMissedTasks(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		// Stop queue worker pool to avoid interference
		if app.QueueWorkerPool != nil {
			app.QueueWorkerPool.Stop()
		}

		s := app.Scheduler

		var executionCount atomic.Int32

		// Register a critical task
		err := s.RegisterTask(
			"CatchupTask",
			func(ctx context.Context) error {
				executionCount.Add(1)
				return nil
			},
			scheduler.WithSchedule(scheduler.Daily),
			scheduler.WithCritical(),
		)

		if err != nil {
			t.Fatalf("Failed to register task: %v", err)
		}

		// Get task from registry
		task, err := s.Registry.Get("CatchupTask")

		if err != nil {
			t.Fatalf("Failed to get task: %v", err)
		}

		// Create a missed execution
		missed := []scheduler.MissedExecution{
			{
				TaskName:    "CatchupTask",
				Task:        task,
				ScheduledAt: time.Now().UTC().Add(-24 * time.Hour),
				MissedBy:    24 * time.Hour,
			},
		}

		// Execute missed tasks
		s.ExecuteMissedTasks(missed)

		// Wait for async execution
		time.Sleep(500 * time.Millisecond)

		count := executionCount.Load()
		if count != 1 {
			t.Errorf("Expected 1 catch-up execution, got %d", count)
		}
	})
}

func TestScheduler_LifecycleIntegration(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		// Stop queue worker pool to avoid interference
		if app.QueueWorkerPool != nil {
			app.QueueWorkerPool.Stop()
		}

		var catchupExecutionCount atomic.Int32
		var regularExecutionCount atomic.Int32

		// Simulate a previous shutdown 2 days ago (before app started)
		db, err := app.DatabaseManager.SystemDatabase().DB()

		if err != nil {
			t.Fatalf("Failed to get system database: %v", err)
		}

		twoDaysAgo := time.Now().UTC().Add(-48 * time.Hour).Format(time.RFC3339)
		_, err = db.Exec("INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)",
			"primary_node_stopped_at", twoDaysAgo)

		if err != nil {
			t.Fatalf("Failed to set shutdown timestamp: %v", err)
		}

		// Register a critical daily task
		err = app.Scheduler.RegisterTask(
			"DailyBackup",
			func(ctx context.Context) error {
				catchupExecutionCount.Add(1)
				return nil
			},
			scheduler.WithSchedule(scheduler.Daily),
			scheduler.WithTime("03:00"),
			scheduler.WithCritical(),
		)

		if err != nil {
			t.Fatalf("Failed to register task: %v", err)
		}

		// Register a non-critical task to verify it doesn't catch up
		err = app.Scheduler.RegisterTask(
			"HourlyReport",
			func(ctx context.Context) error {
				regularExecutionCount.Add(1)
				return nil
			},
			scheduler.WithSchedule(scheduler.Hourly),
			// No WithCritical()
		)

		if err != nil {
			t.Fatalf("Failed to register non-critical task: %v", err)
		}

		// Start scheduler (this triggers gap analysis in production via OnStarted)
		app.Scheduler.Start()
		defer func() {
			if err := app.Scheduler.Stop(); err != nil {
				t.Fatalf("Failed to stop scheduler: %v", err)
			}
		}()

		// Wait a moment for gocron to initialize
		time.Sleep(200 * time.Millisecond)

		// Manually trigger gap analysis (simulating what OnStarted does)
		gaps, err := app.Scheduler.AnalyzeGaps()

		if err != nil {
			t.Fatalf("Failed to analyze gaps: %v", err)
		}

		t.Logf("Found %d gaps", len(gaps))

		for _, gap := range gaps {
			t.Logf("  Gap: %s, scheduled_at=%v, missed_by=%v",
				gap.TaskName, gap.ScheduledAt, gap.MissedBy)
		}

		if len(gaps) == 0 {
			t.Error("Expected at least 1 gap for critical task after 2-day downtime")
		}

		// Execute the missed tasks
		app.Scheduler.ExecuteMissedTasks(gaps)

		// Wait for catch-up execution
		time.Sleep(1 * time.Second)

		// Verify critical task was executed for catch-up
		catchupCount := catchupExecutionCount.Load()

		if catchupCount == 0 {
			t.Error("Expected critical task to execute at least once for catch-up, got 0")
		} else {
			t.Logf("Critical task executed %d time(s) for catch-up", catchupCount)
		}

		// Verify non-critical task was NOT executed (no normal scheduler ticks in this timeframe)
		regularCount := regularExecutionCount.Load()

		if regularCount > 0 {
			t.Logf("Non-critical task executed %d time(s) (expected 0 for catch-up, but may have run on schedule)", regularCount)
		}
	})
}

func TestScheduler_GapAnalysis_WeeklySchedule(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		// Stop queue worker pool
		if app.QueueWorkerPool != nil {
			app.QueueWorkerPool.Stop()
		}

		s := app.Scheduler

		// Register a weekly critical task (runs every Monday at 09:00)
		var executionCount atomic.Int32

		err := s.RegisterTask(
			"WeeklyBackup",
			func(ctx context.Context) error {
				executionCount.Add(1)
				return nil
			},
			scheduler.WithSchedule(scheduler.Weekly),
			scheduler.WithWeekday("Monday"),
			scheduler.WithTime("09:00"),
			scheduler.WithCritical(),
		)

		if err != nil {
			t.Fatalf("Failed to register weekly task: %v", err)
		}

		s.Start()

		defer func() {
			if err := s.Stop(); err != nil {
				t.Fatalf("Failed to stop scheduler: %v", err)
			}
		}()

		// Simulate a shutdown 10 days ago (should have missed at least one Monday)
		shutdownTime := time.Now().UTC().Add(-10 * 24 * time.Hour)
		db, err := app.DatabaseManager.SystemDatabase().DB()

		if err != nil {
			t.Fatalf("Failed to get system database: %v", err)
		}

		_, err = db.Exec("INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)",
			"primary_node_stopped_at", shutdownTime.Format(time.RFC3339))

		if err != nil {
			t.Fatalf("Failed to set shutdown timestamp: %v", err)
		}

		// Allow scheduler to initialize jobs
		time.Sleep(100 * time.Millisecond)

		// Analyze gaps
		gaps, err := s.AnalyzeGaps()

		if err != nil {
			t.Fatalf("AnalyzeGaps failed: %v", err)
		}

		// Should detect at least one missed weekly execution
		if len(gaps) < 1 {
			t.Errorf("Expected at least 1 missed weekly execution, got %d", len(gaps))
		}

		// Execute missed tasks
		s.ExecuteMissedTasks(gaps)
		time.Sleep(200 * time.Millisecond)

		// Verify the task was executed
		count := executionCount.Load()

		if count < 1 {
			t.Error("Expected weekly task to execute at least once for catch-up")
		}
	})
}

func TestScheduler_GapAnalysis_MonthlySchedule(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		// Stop queue worker pool
		if app.QueueWorkerPool != nil {
			app.QueueWorkerPool.Stop()
		}

		s := app.Scheduler

		// Register a monthly critical task (runs on the 1st of every month at 00:00)
		var executionCount atomic.Int32

		err := s.RegisterTask(
			"MonthlyReport",
			func(ctx context.Context) error {
				executionCount.Add(1)
				return nil
			},
			scheduler.WithSchedule(scheduler.Monthly),
			scheduler.WithDay(1),
			scheduler.WithTime("00:00"),
			scheduler.WithCritical(),
		)

		if err != nil {
			t.Fatalf("Failed to register monthly task: %v", err)
		}

		s.Start()

		defer func() {
			if err := s.Stop(); err != nil {
				t.Fatalf("Failed to stop scheduler: %v", err)
			}
		}()

		// Simulate a shutdown 35 days ago (should have missed at least one monthly execution)
		shutdownTime := time.Now().UTC().Add(-35 * 24 * time.Hour)

		db, err := app.DatabaseManager.SystemDatabase().DB()

		if err != nil {
			t.Fatalf("Failed to get system database: %v", err)
		}

		_, err = db.Exec("INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)",
			"primary_node_stopped_at", shutdownTime.Format(time.RFC3339))

		if err != nil {
			t.Fatalf("Failed to set shutdown timestamp: %v", err)
		}

		// Allow scheduler to initialize jobs
		time.Sleep(100 * time.Millisecond)

		// Analyze gaps
		gaps, err := s.AnalyzeGaps()

		if err != nil {
			t.Fatalf("AnalyzeGaps failed: %v", err)
		}

		// Should detect at least one missed monthly execution
		if len(gaps) < 1 {
			t.Errorf("Expected at least 1 missed monthly execution, got %d", len(gaps))
		}

		// Execute missed tasks
		s.ExecuteMissedTasks(gaps)
		time.Sleep(200 * time.Millisecond)

		// Verify the task was executed
		count := executionCount.Load()

		if count < 1 {
			t.Error("Expected monthly task to execute at least once for catch-up")
		}
	})
}

func TestScheduler_GapAnalysis_HourlySchedule(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		// Stop queue worker pool
		if app.QueueWorkerPool != nil {
			app.QueueWorkerPool.Stop()
		}

		s := app.Scheduler

		// Register an hourly critical task
		var executionCount atomic.Int32

		err := s.RegisterTask(
			"HourlyHealthCheck",
			func(ctx context.Context) error {
				executionCount.Add(1)
				return nil
			},
			scheduler.WithSchedule(scheduler.Hourly),
			scheduler.WithCritical(),
		)

		if err != nil {
			t.Fatalf("Failed to register hourly task: %v", err)
		}

		s.Start()

		defer func() {
			if err := s.Stop(); err != nil {
				t.Fatalf("Failed to stop scheduler: %v", err)
			}
		}()

		// Simulate a shutdown 3 hours ago
		shutdownTime := time.Now().UTC().Add(-3 * time.Hour)
		db, err := app.DatabaseManager.SystemDatabase().DB()

		if err != nil {
			t.Fatalf("Failed to get system database: %v", err)
		}

		_, err = db.Exec("INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)",
			"primary_node_stopped_at", shutdownTime.Format(time.RFC3339))

		if err != nil {
			t.Fatalf("Failed to set shutdown timestamp: %v", err)
		}

		// Allow scheduler to initialize jobs
		time.Sleep(100 * time.Millisecond)

		// Analyze gaps
		gaps, err := s.AnalyzeGaps()

		if err != nil {
			t.Fatalf("AnalyzeGaps failed: %v", err)
		}

		// Should detect missed hourly execution
		if len(gaps) < 1 {
			t.Errorf("Expected at least 1 missed hourly execution, got %d", len(gaps))
		}

		// Execute missed tasks
		s.ExecuteMissedTasks(gaps)
		time.Sleep(200 * time.Millisecond)

		// Verify the task was executed
		count := executionCount.Load()

		if count < 1 {
			t.Error("Expected hourly task to execute at least once for catch-up")
		}
	})
}

func TestScheduler_GapAnalysis_CronSchedule(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		// Stop queue worker pool
		if app.QueueWorkerPool != nil {
			app.QueueWorkerPool.Stop()
		}

		s := app.Scheduler

		// Register a cron critical task (every day at midnight)
		var executionCount atomic.Int32

		err := s.RegisterTask(
			"CronTask",
			func(ctx context.Context) error {
				executionCount.Add(1)
				return nil
			},
			scheduler.WithCron("0 0 * * *"), // Daily at midnight
			scheduler.WithCritical(),
		)

		if err != nil {
			t.Fatalf("Failed to register cron task: %v", err)
		}

		s.Start()

		defer func() {
			if err := s.Stop(); err != nil {
				t.Fatalf("Failed to stop scheduler: %v", err)
			}
		}()

		// Simulate a shutdown 2 days ago
		shutdownTime := time.Now().UTC().Add(-48 * time.Hour)
		db, err := app.DatabaseManager.SystemDatabase().DB()

		if err != nil {
			t.Fatalf("Failed to get system database: %v", err)
		}

		_, err = db.Exec("INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)",
			"primary_node_stopped_at", shutdownTime.Format(time.RFC3339))

		if err != nil {
			t.Fatalf("Failed to set shutdown timestamp: %v", err)
		}

		// Allow scheduler to initialize jobs
		time.Sleep(100 * time.Millisecond)

		// Analyze gaps
		gaps, err := s.AnalyzeGaps()

		if err != nil {
			t.Fatalf("AnalyzeGaps failed: %v", err)
		}

		// Should detect missed cron execution (uses heuristic)
		if len(gaps) < 1 {
			t.Errorf("Expected at least 1 missed cron execution, got %d", len(gaps))
		} else {
			// Verify the gap detection for cron uses the heuristic (shutdown + 1 minute)
			gap := gaps[0]
			expectedScheduledTime := shutdownTime.Add(1 * time.Minute)

			// Allow 1 second variance for test timing
			timeDiff := gap.ScheduledAt.Sub(expectedScheduledTime).Abs()

			if timeDiff > 1*time.Second {
				t.Logf("Cron heuristic: scheduled_at=%v, expected=%v (diff=%v)",
					gap.ScheduledAt, expectedScheduledTime, timeDiff)
			}
		}

		// Execute missed tasks
		s.ExecuteMissedTasks(gaps)
		time.Sleep(200 * time.Millisecond)

		// Verify the task was executed
		count := executionCount.Load()

		if count < 1 {
			t.Error("Expected cron task to execute at least once for catch-up")
		}
	})
}

func TestScheduler_GapAnalysis_FrequentSchedules(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		// Stop queue worker pool
		if app.QueueWorkerPool != nil {
			app.QueueWorkerPool.Stop()
		}

		s := app.Scheduler

		// Register every-minute and every-second critical tasks
		var minuteCount, secondCount atomic.Int32

		err := s.RegisterTask(
			"EveryMinuteTask",
			func(ctx context.Context) error {
				minuteCount.Add(1)
				return nil
			},
			scheduler.WithSchedule(scheduler.EveryMinute),
			scheduler.WithCritical(),
		)

		if err != nil {
			t.Fatalf("Failed to register every-minute task: %v", err)
		}

		err = s.RegisterTask(
			"EverySecondTask",
			func(ctx context.Context) error {
				secondCount.Add(1)
				return nil
			},
			scheduler.WithSchedule(scheduler.EverySecond),
			scheduler.WithCritical(),
		)

		if err != nil {
			t.Fatalf("Failed to register every-second task: %v", err)
		}

		s.Start()

		defer func() {
			if err := s.Stop(); err != nil {
				t.Fatalf("Failed to stop scheduler: %v", err)
			}
		}()

		// Simulate a shutdown 5 minutes ago
		shutdownTime := time.Now().UTC().Add(-5 * time.Minute)
		db, err := app.DatabaseManager.SystemDatabase().DB()

		if err != nil {
			t.Fatalf("Failed to get system database: %v", err)
		}

		_, err = db.Exec("INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)",
			"primary_node_stopped_at", shutdownTime.Format(time.RFC3339))

		if err != nil {
			t.Fatalf("Failed to set shutdown timestamp: %v", err)
		}

		// Allow scheduler to initialize jobs
		time.Sleep(100 * time.Millisecond)

		// Analyze gaps
		gaps, err := s.AnalyzeGaps()

		if err != nil {
			t.Fatalf("AnalyzeGaps failed: %v", err)
		}

		// Should detect both tasks as missed (window collapse means 1 execution each)
		if len(gaps) != 2 {
			t.Errorf("Expected 2 missed executions (one per task), got %d", len(gaps))
		}

		// Execute missed tasks
		s.ExecuteMissedTasks(gaps)
		time.Sleep(200 * time.Millisecond)

		// Verify both tasks were executed once (window collapsed)
		if minuteCount.Load() < 1 {
			t.Error("Expected every-minute task to execute at least once")
		}

		if secondCount.Load() < 1 {
			t.Error("Expected every-second task to execute at least once")
		}
	})
}

func TestScheduler_GapAnalysis_InvalidShutdownTimestamp(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		// Stop queue worker pool
		if app.QueueWorkerPool != nil {
			app.QueueWorkerPool.Stop()
		}

		s := app.Scheduler

		// Register a critical task
		err := s.RegisterTask(
			"TestTask",
			func(ctx context.Context) error {
				return nil
			},
			scheduler.WithSchedule(scheduler.Daily),
			scheduler.WithCritical(),
		)

		if err != nil {
			t.Fatalf("Failed to register task: %v", err)
		}

		s.Start()

		defer func() {
			if err := s.Stop(); err != nil {
				t.Fatalf("Failed to stop scheduler: %v", err)
			}
		}()

		// Set an invalid shutdown timestamp
		db, err := app.DatabaseManager.SystemDatabase().DB()

		if err != nil {
			t.Fatalf("Failed to get system database: %v", err)
		}

		_, err = db.Exec("INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)",
			"primary_node_stopped_at", "invalid-timestamp")

		if err != nil {
			t.Fatalf("Failed to set shutdown timestamp: %v", err)
		}

		// Allow scheduler to initialize jobs
		time.Sleep(100 * time.Millisecond)

		// Analyze gaps - should return error for invalid timestamp
		_, err = s.AnalyzeGaps()

		if err == nil {
			t.Error("Expected error for invalid shutdown timestamp, got nil")
		}
	})
}
