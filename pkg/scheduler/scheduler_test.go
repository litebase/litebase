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

func TestScheduler_EverySecond(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		// Stop queue worker pool to avoid interference
		if app.QueueWorkerPool != nil {
			app.QueueWorkerPool.Stop()
		}

		var executionCount atomic.Int32

		s := app.Scheduler

		err := s.RegisterTask(
			"TestEverySecond",
			func(ctx context.Context) error {
				executionCount.Add(1)
				return nil
			},
			scheduler.WithSchedule(scheduler.EverySecond),
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

		time.Sleep(4500 * time.Millisecond)

		count := executionCount.Load()

		if count < 3 {
			t.Errorf("Expected at least 3 executions, got %d", count)
		}
	})
}

func TestScheduler_EveryMinute(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		// Stop queue worker pool to avoid interference
		if app.QueueWorkerPool != nil {
			app.QueueWorkerPool.Stop()
		}

		s := app.Scheduler

		err := s.RegisterTask(
			"TestEveryMinute",
			func(ctx context.Context) error {
				return nil
			},
			scheduler.WithSchedule(scheduler.EveryMinute),
		)

		if err != nil {
			t.Fatalf("Failed to register task: %v", err)
		}

		// Just verify it registers successfully
		task, err := s.Registry.Get("TestEveryMinute")
		if err != nil {
			t.Fatalf("Task not found in registry: %v", err)
		}

		if task.Schedule != scheduler.EveryMinute {
			t.Errorf("Expected EveryMinute schedule, got %s", task.Schedule)
		}
	})
}

func TestScheduler_PrimaryOnly(t *testing.T) {
	test.Run(t, func() {
		// Create two servers - one will be primary, one replica
		server1 := test.NewTestServer(t)
		server2 := test.NewTestServer(t)
		defer server1.Shutdown()
		defer server2.Shutdown()

		// Use the replica server's scheduler
		var executionCount atomic.Int32

		s := server2.App.Scheduler

		err := s.RegisterTask(
			"TestPrimaryOnly",
			func(ctx context.Context) error {
				executionCount.Add(1)
				return nil
			},
			scheduler.WithSchedule(scheduler.EverySecond),
		)

		if err != nil {
			t.Fatalf("Failed to register task: %v", err)
		}

		// Scheduler is already started by the app, just wait
		time.Sleep(3 * time.Second)

		count := executionCount.Load()
		if count != 0 {
			t.Errorf("Expected 0 executions on replica, got %d", count)
		}
	})
}

func TestScheduler_WithoutOverlap(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		// Stop queue worker pool to avoid interference
		if app.QueueWorkerPool != nil {
			app.QueueWorkerPool.Stop()
		}

		var executionCount atomic.Int32
		var concurrentExecutions int32

		s := app.Scheduler

		err := s.RegisterTask(
			"TestOverlap",
			func(ctx context.Context) error {
				concurrent := atomic.AddInt32(&concurrentExecutions, 1)
				time.Sleep(2 * time.Second)
				atomic.AddInt32(&concurrentExecutions, -1)
				executionCount.Add(1)

				if concurrent > 1 {
					t.Errorf("Concurrent executions detected: %d", concurrent)
				}

				return nil
			},
			scheduler.WithSchedule(scheduler.EverySecond),
			scheduler.WithoutOverlap(),
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

		time.Sleep(5 * time.Second)

		count := executionCount.Load()

		if count > 3 {
			t.Errorf("Expected at most 3 executions with overlap prevention, got %d", count)
		}
	})
}

func TestScheduler_Daily(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		// Stop queue worker pool to avoid interference
		if app.QueueWorkerPool != nil {
			app.QueueWorkerPool.Stop()
		}

		s := app.Scheduler

		err := s.RegisterTask(
			"TestDaily",
			func(ctx context.Context) error {
				return nil
			},
			scheduler.WithSchedule(scheduler.Daily),
			scheduler.WithTime("14:30"),
		)

		if err != nil {
			t.Fatalf("Failed to register task: %v", err)
		}

		// Just verify it registers successfully
		task, err := s.Registry.Get("TestDaily")
		if err != nil {
			t.Fatalf("Task not found in registry: %v", err)
		}

		if task.Time != "14:30" {
			t.Errorf("Expected time 14:30, got %s", task.Time)
		}
	})
}

func TestScheduler_Cron(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		// Stop queue worker pool to avoid interference
		if app.QueueWorkerPool != nil {
			app.QueueWorkerPool.Stop()
		}

		var executionCount atomic.Int32

		s := app.Scheduler

		// Test "every 5 seconds" using cron (closest we can get is */1 * * * * with seconds support)
		// But standard cron doesn't support seconds, so let's test "every minute"
		err := s.RegisterTask(
			"TestCron",
			func(ctx context.Context) error {
				executionCount.Add(1)
				return nil
			},
			scheduler.WithCron("* * * * *"), // Every minute
		)

		if err != nil {
			t.Fatalf("Failed to register task with cron: %v", err)
		}

		// Just verify it registers successfully
		task, err := s.Registry.Get("TestCron")
		if err != nil {
			t.Fatalf("Task not found in registry: %v", err)
		}

		if task.CronExpr != "* * * * *" {
			t.Errorf("Expected cron expr '* * * * *', got %s", task.CronExpr)
		}
	})
}

func TestScheduler_ReplicaPromotedToPrimary(t *testing.T) {
	test.Run(t, func() {
		server1 := test.NewTestServer(t)
		server2 := test.NewTestServer(t)
		defer server2.Shutdown()

		var executionCount atomic.Int32

		// Stop server2's app scheduler if it exists
		if server2.App.Scheduler != nil {
			if err := server2.App.Scheduler.Stop(); err != nil {
				t.Fatalf("Failed to stop server2's scheduler: %v", err)
			}
		}

		// Create a new scheduler with server2's system database and isPrimary check
		s := scheduler.NewScheduler(
			server2.App.DatabaseManager.SystemDatabase(),
			server2.App.Cluster.Node().IsPrimary,
		)

		err := s.RegisterTask(
			"TestPromotion",
			func(ctx context.Context) error {
				executionCount.Add(1)
				return nil
			},
			scheduler.WithSchedule(scheduler.EverySecond),
		)

		if err != nil {
			t.Fatalf("Failed to register task: %v", err)
		}

		// Start the scheduler explicitly
		s.Start()

		defer func() {
			if err := s.Stop(); err != nil {
				t.Fatalf("Failed to stop scheduler: %v", err)
			}
		}()

		// Wait as replica - should not execute
		time.Sleep(2 * time.Second)

		count := executionCount.Load()
		if count != 0 {
			t.Errorf("Expected 0 executions while replica, got %d", count)
		}

		// Promote to primary
		server1.Shutdown()

		// Wait for server2 to detect it's primary and start executing
		time.Sleep(5 * time.Second)

		count = executionCount.Load()

		if count < 2 {
			t.Errorf("Expected at least 2 executions after promotion, got %d", count)
		}
	})
}
