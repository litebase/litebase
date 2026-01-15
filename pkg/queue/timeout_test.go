package queue_test

import (
	"context"
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/queue"
)

func TestWorker_JobTimeout(t *testing.T) {
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

		jobStarted := false
		jobCancelled := false

		handler := func(ctx context.Context, data map[string]any) error {
			jobStarted = true

			// Simulate a long-running job
			select {
			case <-time.After(5 * time.Second):
				// Job completed normally (should not happen)
				return nil
			case <-ctx.Done():
				// Job was cancelled due to timeout
				jobCancelled = true
				return ctx.Err()
			}
		}

		err := pool.RegisterJob(
			"TimeoutJob",
			handler,
			queue.WithQueue("default"),
			queue.WithRetries(0, 0), // No retries
			queue.WithTimeout(500*time.Millisecond),
		)

		if err != nil {
			t.Fatalf("Failed to register job: %v", err)
		}

		if err := pool.Start(); err != nil {
			t.Fatalf("Failed to start pool: %v", err)
		}

		defer pool.Stop()

		// Use the pool's dispatcher which knows about the registered jobs
		dispatcher := pool.NewDispatcher()

		jobID, err := dispatcher.DispatchJob("TimeoutJob", map[string]any{
			"key": "timeout-test",
		})

		if err != nil {
			t.Fatalf("Failed to dispatch job: %v", err)
		}

		// Wait for the job to timeout and be marked as failed
		time.Sleep(3 * time.Second)

		// Verify the job started
		if !jobStarted {
			t.Error("Expected job to start")
		}

		// Verify the job was cancelled due to timeout
		if !jobCancelled {
			t.Error("Expected job to be cancelled due to timeout")
		}

		// Verify the job status in the database
		db, err := systemDB.DB()

		if err != nil {
			t.Fatalf("Failed to get database: %v", err)
		}

		var status string
		var attempts int
		var maxAttempts int

		err = db.QueryRow(`
			SELECT status, attempts, max_attempts FROM queued_jobs WHERE id = ?
		`, jobID).Scan(&status, &attempts, &maxAttempts)

		if err != nil {
			t.Fatalf("Failed to query job: %v", err)
		}

		t.Logf("Job status: %s, attempts: %d, max_attempts: %d", status, attempts, maxAttempts)

		// The job should be marked as failed after exceeding max attempts (0)
		if status != string(queue.JobStatusFailed) {
			t.Errorf("Expected status failed, got %s", status)
		}

		// Should have 1 attempt (initial attempt, no retries)
		if attempts != 1 {
			t.Errorf("Expected 1 attempt, got %d", attempts)
		}
	})
}

func TestWorker_JobTimeoutWithRetry(t *testing.T) {
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

		attemptCount := 0

		handler := func(ctx context.Context, data map[string]any) error {
			attemptCount++

			// First two attempts timeout, third succeeds
			if attemptCount < 3 {
				select {
				case <-time.After(5 * time.Second):
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			}

			// Third attempt succeeds quickly
			return nil
		}

		err := pool.RegisterJob(
			"TimeoutRetryJob",
			handler,
			queue.WithQueue("default"),
			queue.WithRetries(3, 200*time.Millisecond),
			queue.WithTimeout(300*time.Millisecond),
		)

		if err != nil {
			t.Fatalf("Failed to register job: %v", err)
		}

		if err := pool.Start(); err != nil {
			t.Fatalf("Failed to start pool: %v", err)
		}

		defer pool.Stop()

		// Use the pool's dispatcher which knows about the registered jobs
		dispatcher := pool.NewDispatcher()

		jobID, err := dispatcher.DispatchJob("TimeoutRetryJob", map[string]any{
			"key": "timeout-retry-test",
		})

		if err != nil {
			t.Fatalf("Failed to dispatch job: %v", err)
		}

		// Wait for retries and final success
		time.Sleep(5 * time.Second)

		// Verify the job was attempted 3 times (2 timeouts + 1 success)
		if attemptCount != 3 {
			t.Errorf("Expected 3 attempts, got %d", attemptCount)
		}

		// Verify the job status in the database
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

		// The job should be marked as completed after retries
		if status != string(queue.JobStatusCompleted) {
			t.Errorf("Expected status completed, got %s", status)
		}

		// Should have 2 attempts (failures) before the final success
		if attempts != 2 {
			t.Errorf("Expected 2 attempts, got %d", attempts)
		}
	})
}

func TestWorker_NoTimeout(t *testing.T) {
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

		jobCompleted := false

		handler := func(ctx context.Context, data map[string]any) error {
			// Simulate a long-running job that should complete
			time.Sleep(300 * time.Millisecond)
			jobCompleted = true
			return nil
		}

		// Register without timeout (default is 0 = no timeout)
		err := pool.RegisterJob(
			"NoTimeoutJob",
			handler,
			queue.WithQueue("default"),
			queue.WithRetries(0, 0),
		)

		if err != nil {
			t.Fatalf("Failed to register job: %v", err)
		}

		if err := pool.Start(); err != nil {
			t.Fatalf("Failed to start pool: %v", err)
		}

		defer pool.Stop()

		// Use the pool's dispatcher which knows about the registered jobs
		dispatcher := pool.NewDispatcher()

		jobID, err := dispatcher.DispatchJob("NoTimeoutJob", map[string]any{
			"key": "no-timeout-test",
		})

		if err != nil {
			t.Fatalf("Failed to dispatch job: %v", err)
		}

		// Wait for the job to complete
		time.Sleep(2 * time.Second)

		// Verify the job completed
		if !jobCompleted {
			t.Error("Expected job to complete without timeout")
		}

		// Verify the job status in the database
		db, err := systemDB.DB()

		if err != nil {
			t.Fatalf("Failed to get database: %v", err)
		}

		var status string

		err = db.QueryRow(`
			SELECT status FROM queued_jobs WHERE id = ?
		`, jobID).Scan(&status)

		if err != nil {
			t.Fatalf("Failed to query job: %v", err)
		}

		if status != string(queue.JobStatusCompleted) {
			t.Errorf("Expected status completed, got %s", status)
		}
	})
}
