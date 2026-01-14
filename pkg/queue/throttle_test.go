package queue_test

import (
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/queue"
)

func TestWorker_JobThrottled(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		if server.App.QueueWorkerPool != nil {
			server.App.QueueWorkerPool.Stop()
		}

		systemDB := server.App.DatabaseManager.SystemDatabase()

		pool := queue.NewWorkerPool(systemDB, server.App.Cluster, queue.WorkerPoolConfig{
			WorkerCount: 1,
		})

		throttleCheckCount := 0
		executionCount := 0

		handler := func(data map[string]any) error {
			executionCount++

			return nil
		}

		err := pool.RegisterJob(
			"ThrottledJob",
			handler,
			queue.WithQueue("default"),
			queue.WithRetries(3, 5*time.Second),
			queue.WithThrottle(func(data map[string]any, key string) (bool, time.Duration) {
				throttleCheckCount++

				if throttleCheckCount == 1 {
					return true, 500 * time.Millisecond
				}

				return false, 0
			}),
		)

		if err != nil {
			t.Fatalf("Failed to register job: %v", err)
		}

		if err := pool.Start(); err != nil {
			t.Fatalf("Failed to start pool: %v", err)
		}

		defer pool.Stop()

		dispatcher := server.App.QueueDispatcher

		jobID, err := dispatcher.DispatchJob("ThrottledJob", map[string]any{
			"test": "data",
		})

		if err != nil {
			t.Fatalf("Failed to dispatch job: %v", err)
		}

		// Wait longer for job to be throttled and then processed
		time.Sleep(3 * time.Second)

		if throttleCheckCount < 2 {
			t.Errorf("Expected throttle to be checked at least twice, got %d", throttleCheckCount)
		}

		if executionCount != 1 {
			t.Errorf("Expected job to execute once, got %d", executionCount)
		}

		db, err := systemDB.DB()

		if err != nil {
			t.Fatalf("Failed to get database: %v", err)
		}

		var status string
		var attempts int

		err = db.QueryRow(`
			SELECT status, attempts FROM queued_jobs WHERE id = ?
		`, jobID).Scan(&status, &attempts)

		if err != nil {
			t.Fatalf("Failed to query job: %v", err)
		}

		if status != string(queue.JobStatusCompleted) {
			t.Errorf("Expected status completed, got %s", status)
		}

		if attempts != 0 {
			t.Errorf("Expected 0 attempts (throttling doesn't count), got %d", attempts)
		}
	})
}

func TestWorker_JobThrottledByKey(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		if server.App.QueueWorkerPool != nil {
			server.App.QueueWorkerPool.Stop()
		}

		systemDB := server.App.DatabaseManager.SystemDatabase()

		pool := queue.NewWorkerPool(systemDB, server.App.Cluster, queue.WorkerPoolConfig{
			WorkerCount: 1,
		})

		executedKeys := make(map[string]bool)

		handler := func(data map[string]any) error {
			if key, ok := data["key"].(string); ok {
				executedKeys[key] = true
			}

			return nil
		}

		err := pool.RegisterJob(
			"KeyThrottledJob",
			handler,
			queue.WithQueue("default"),
			queue.WithRetries(3, 5*time.Second),
			queue.WithThrottle(func(data map[string]any, key string) (bool, time.Duration) {
				if key == "throttle-me" {
					return true, 1 * time.Second
				}

				return false, 0
			}),
		)

		if err != nil {
			t.Fatalf("Failed to register job: %v", err)
		}

		if err := pool.Start(); err != nil {
			t.Fatalf("Failed to start pool: %v", err)
		}

		defer pool.Stop()

		dispatcher := server.App.QueueDispatcher

		_, err = dispatcher.DispatchJob("KeyThrottledJob", map[string]any{
			"key": "allow-me",
		}, queue.WithKey("allow-me"))

		if err != nil {
			t.Fatalf("Failed to dispatch job 1: %v", err)
		}

		_, err = dispatcher.DispatchJob("KeyThrottledJob", map[string]any{
			"key": "throttle-me",
		}, queue.WithKey("throttle-me"))

		if err != nil {
			t.Fatalf("Failed to dispatch job 2: %v", err)
		}

		time.Sleep(2500 * time.Millisecond)

		if !executedKeys["allow-me"] {
			t.Error("Expected 'allow-me' job to execute")
		}

		if !executedKeys["throttle-me"] {
			t.Error("Expected 'throttle-me' job to eventually execute after throttle")
		}
	})
}
