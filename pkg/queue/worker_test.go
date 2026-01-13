package queue_test

import (
	"errors"
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/queue"
)

func TestWorker_ProcessJobSuccess(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		// Stop app's worker pool to avoid conflicts with test workers
		if server.App.QueueWorkerPool != nil {
			server.App.QueueWorkerPool.Stop()
		}

		systemDB := server.App.DatabaseManager.SystemDatabase()
		dispatcher := server.App.QueueDispatcher
		registry := queue.NewJobRegistry()

		// Register a successful job
		registry.Register("test-job", func(key string) (queue.Job, error) {
			return &TestJob{
				name:        "Test Job",
				key:         key,
				queueName:   "default",
				jobType:     "test-job",
				retries:     3,
				retryAfter:  5 * time.Second,
				handleError: nil, // Success
			}, nil
		})

		// Dispatch a job first to get the ID
		job := &TestJob{
			name:       "Test Job",
			key:        "test-key-1",
			queueName:  "default",
			jobType:    "test-job",
			retries:    3,
			retryAfter: 5 * time.Second,
		}

		id, err := dispatcher.Dispatch(job)

		if err != nil {
			t.Fatalf("Failed to dispatch job: %v", err)
		}

		// Create and start worker with hook
		worker := queue.NewWorker("worker-1", systemDB, registry)

		// Use a channel to wait for job completion
		done := make(chan error, 1)

		worker.SetAfterJobHook(func(jobID int64, status queue.JobStatus, err error) {
			if jobID == id {
				done <- err
			}
		})

		worker.Start()
		defer worker.Stop()

		// Wait for job to be completed via hook
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("Job failed: %v", err)
			}

			// Now verify the database state
			db, err := systemDB.DB()

			if err != nil {
				t.Fatalf("Failed to get database: %v", err)
			}

			var status string
			var completedAt *string

			err = db.QueryRow(`
				SELECT status, completed_at FROM queued_jobs WHERE id = ?
			`, id).Scan(&status, &completedAt)

			if err != nil {
				t.Fatalf("Failed to query job: %v", err)
			}

			if status != string(queue.JobStatusCompleted) {
				t.Errorf("Expected status completed, got %s", status)
			}

			if completedAt == nil {
				t.Error("Expected completed_at to be set")
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Timeout waiting for job to be completed")
		}
	})
}

func TestWorker_ProcessJobFailure(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		// Stop app's worker pool to avoid conflicts with test workers
		if server.App.QueueWorkerPool != nil {
			server.App.QueueWorkerPool.Stop()
		}

		systemDB := server.App.DatabaseManager.SystemDatabase()
		dispatcher := server.App.QueueDispatcher
		registry := queue.NewJobRegistry()

		// Register a failing job
		registry.Register("failing-job", func(key string) (queue.Job, error) {
			return &TestJob{
				name:        "Failing Job",
				key:         key,
				queueName:   "default",
				jobType:     "failing-job",
				retries:     0, // No retries
				retryAfter:  5 * time.Second,
				handleError: errors.New("job failed"),
			}, nil
		})

		// Dispatch a job
		job := &TestJob{
			name:       "Failing Job",
			key:        "failing-key-1",
			queueName:  "default",
			jobType:    "failing-job",
			retries:    0, // No retries
			retryAfter: 5 * time.Second,
		}

		id, err := dispatcher.Dispatch(job)

		if err != nil {
			t.Fatalf("Failed to dispatch job: %v", err)
		}

		// Create and start worker with hook
		worker := queue.NewWorker("worker-1", systemDB, registry)

		done := make(chan error, 1)

		worker.SetAfterJobHook(func(jobID int64, status queue.JobStatus, err error) {
			if jobID == id {
				done <- err
			}
		})

		worker.Start()
		defer worker.Stop()

		// Wait for job to fail via hook
		select {
		case err := <-done:
			if err == nil {
				t.Fatal("Expected job to fail")
			}

			db, err := systemDB.DB()

			if err != nil {
				t.Fatalf("Failed to get database: %v", err)
			}

			var status string
			var errorLog *string

			err = db.QueryRow(`
				SELECT status, error_log FROM queued_jobs WHERE id = ?
			`, id).Scan(&status, &errorLog)

			if err != nil {
				t.Fatalf("Failed to query job: %v", err)
			}

			if status != string(queue.JobStatusFailed) {
				t.Errorf("Expected status failed, got %s", status)
			}

			if errorLog == nil {
				t.Error("Expected error_log to be set, but it was NULL")
			} else if *errorLog != "job failed" {
				t.Errorf("Expected error_log 'job failed', got '%s'", *errorLog)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Timeout waiting for job to fail")
		}
	})
}

func TestWorker_ProcessJobRetry(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		// Stop app's worker pool to avoid conflicts with test workers
		if server.App.QueueWorkerPool != nil {
			server.App.QueueWorkerPool.Stop()
		}

		systemDB := server.App.DatabaseManager.SystemDatabase()
		dispatcher := server.App.QueueDispatcher
		registry := queue.NewJobRegistry()

		// Track attempts
		attemptCount := 0

		// Register a job that fails twice then succeeds
		registry.Register("retry-job", func(key string) (queue.Job, error) {
			attemptCount++
			var handleErr error
			if attemptCount < 3 {
				handleErr = errors.New("temporary failure")
			}

			return &TestJob{
				name:        "Retry Job",
				key:         key,
				queueName:   "default",
				jobType:     "retry-job",
				retries:     3,
				retryAfter:  500 * time.Millisecond,
				handleError: handleErr,
			}, nil
		})

		// Dispatch a job
		job := &TestJob{
			name:       "Retry Job",
			key:        "retry-key-1",
			queueName:  "default",
			jobType:    "retry-job",
			retries:    3,
			retryAfter: 500 * time.Millisecond,
		}

		id, err := dispatcher.Dispatch(job)

		if err != nil {
			t.Fatalf("Failed to dispatch job: %v", err)
		}

		// Create and start worker with hook
		worker := queue.NewWorker("worker-1", systemDB, registry)

		done := make(chan error, 1)
		worker.SetAfterJobHook(func(jobID int64, status queue.JobStatus, err error) {
			if jobID == id && status == queue.JobStatusCompleted {
				done <- err
			}
		})

		worker.Start()
		defer worker.Stop()

		// Wait for job to complete after retries
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("Job failed: %v", err)
			}

			db, err := systemDB.DB()
			if err != nil {
				t.Fatalf("Failed to get database: %v", err)
			}

			var status string
			var attempts int
			err = db.QueryRow(`
				SELECT status, attempts FROM queued_jobs WHERE id = ?
			`, id).Scan(&status, &attempts)

			if err != nil {
				t.Fatalf("Failed to query job: %v", err)
			}

			if status != string(queue.JobStatusCompleted) {
				t.Errorf("Expected status completed, got %s", status)
			}

			if attempts != 2 {
				t.Errorf("Expected 2 failed attempts before success, got %d", attempts)
			}

			if attemptCount != 3 {
				t.Errorf("Expected 3 total execution attempts, got %d", attemptCount)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("Timeout waiting for job to complete, attempts: %d", attemptCount)
		}
	})
}

func TestWorker_ProcessJobMaxRetries(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		// Stop app's worker pool to avoid conflicts with test workers
		if server.App.QueueWorkerPool != nil {
			server.App.QueueWorkerPool.Stop()
		}

		systemDB := server.App.DatabaseManager.SystemDatabase()
		dispatcher := server.App.QueueDispatcher
		registry := queue.NewJobRegistry()

		// Register a job that always fails
		registry.Register("always-fail-job", func(key string) (queue.Job, error) {
			return &TestJob{
				name:        "Always Fail Job",
				key:         key,
				queueName:   "default",
				jobType:     "always-fail-job",
				retries:     2,
				retryAfter:  200 * time.Millisecond,
				handleError: errors.New("persistent failure"),
			}, nil
		})

		// Dispatch a job
		job := &TestJob{
			name:       "Always Fail Job",
			key:        "fail-key-1",
			queueName:  "default",
			jobType:    "always-fail-job",
			retries:    2,
			retryAfter: 200 * time.Millisecond,
		}

		id, err := dispatcher.Dispatch(job)

		if err != nil {
			t.Fatalf("Failed to dispatch job: %v", err)
		}

		// Create and start worker with hook
		worker := queue.NewWorker("worker-1", systemDB, registry)

		done := make(chan error, 1)

		worker.SetAfterJobHook(func(jobID int64, status queue.JobStatus, err error) {
			if jobID == id && status == queue.JobStatusFailed {
				done <- err
			}
		})

		worker.Start()
		defer worker.Stop()

		// Wait for job to fail permanently
		select {
		case err := <-done:
			if err == nil {
				t.Fatal("Expected job to fail")
			}

			db, err := systemDB.DB()

			if err != nil {
				t.Fatalf("Failed to get database: %v", err)
			}

			var status string
			var attempts int
			var errorLog *string

			err = db.QueryRow(`
				SELECT status, attempts, error_log FROM queued_jobs WHERE id = ?
			`, id).Scan(&status, &attempts, &errorLog)

			if err != nil {
				t.Fatalf("Failed to query job: %v", err)
			}

			if status != string(queue.JobStatusFailed) {
				t.Errorf("Expected status failed, got %s", status)
			}

			if attempts != 2 {
				t.Errorf("Expected 2 attempts (max retries), got %d", attempts)
			}

			if errorLog == nil {
				t.Error("Expected error_log to be set")
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Timeout waiting for job to fail permanently")
		}
	})
}

func TestWorker_JobTypeNotRegistered(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		// Stop app's worker pool to avoid conflicts with test workers
		if server.App.QueueWorkerPool != nil {
			server.App.QueueWorkerPool.Stop()
		}

		systemDB := server.App.DatabaseManager.SystemDatabase()
		dispatcher := server.App.QueueDispatcher
		registry := queue.NewJobRegistry()

		// Don't register the job type

		// Dispatch a job with unregistered type
		job := &TestJob{
			name:       "Unregistered Job",
			key:        "unregistered-key-1",
			queueName:  "default",
			jobType:    "unregistered-job",
			retries:    3,
			retryAfter: 5 * time.Second,
		}

		id, err := dispatcher.Dispatch(job)

		if err != nil {
			t.Fatalf("Failed to dispatch job: %v", err)
		}

		// Create and start worker with hook
		worker := queue.NewWorker("worker-1", systemDB, registry)

		done := make(chan error, 1)

		worker.SetAfterJobHook(func(jobID int64, status queue.JobStatus, err error) {
			if jobID == id {
				done <- err
			}
		})

		worker.Start()
		defer worker.Stop()

		// Wait for job to fail due to unregistered type
		select {
		case err := <-done:
			if err == nil {
				t.Fatal("Expected job to fail")
			}

			db, err := systemDB.DB()

			if err != nil {
				t.Fatalf("Failed to get database: %v", err)
			}

			var status string
			var errorLog *string

			err = db.QueryRow(`
				SELECT status, error_log FROM queued_jobs WHERE id = ?
			`, id).Scan(&status, &errorLog)

			if err != nil {
				t.Fatalf("Failed to query job: %v", err)
			}

			if status != string(queue.JobStatusFailed) {
				t.Errorf("Expected status failed, got %s", status)
			}

			if errorLog == nil {
				t.Error("Expected error_log to be set")
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Timeout waiting for job to fail")
		}
	})
}

func TestWorker_DelayedJob(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		// Stop app's worker pool to avoid conflicts with test workers
		if server.App.QueueWorkerPool != nil {
			server.App.QueueWorkerPool.Stop()
		}

		systemDB := server.App.DatabaseManager.SystemDatabase()
		dispatcher := server.App.QueueDispatcher
		registry := queue.NewJobRegistry()

		// Register a successful job
		registry.Register("delayed-job", func(key string) (queue.Job, error) {
			return &TestJob{
				name:       "Delayed Job",
				key:        key,
				queueName:  "default",
				jobType:    "delayed-job",
				retries:    3,
				retryAfter: 5 * time.Second,
			}, nil
		})

		// Dispatch a delayed job
		job := &TestJob{
			name:       "Delayed Job",
			key:        "delayed-key-1",
			queueName:  "default",
			jobType:    "delayed-job",
			retries:    3,
			retryAfter: 5 * time.Second,
		}

		delay := 2 * time.Second
		id, err := dispatcher.DispatchWithDelay(job, delay)

		if err != nil {
			t.Fatalf("Failed to dispatch delayed job: %v", err)
		}

		// Create and start worker with hook
		worker := queue.NewWorker("worker-1", systemDB, registry)

		done := make(chan error, 1)

		worker.SetAfterJobHook(func(jobID int64, status queue.JobStatus, err error) {
			if jobID == id {
				done <- err
			}
		})

		worker.Start()
		defer worker.Stop()

		// Job should not be processed immediately
		db, err := systemDB.DB()

		if err != nil {
			t.Fatalf("Failed to get database: %v", err)
		}

		time.Sleep(500 * time.Millisecond)

		var status string

		err = db.QueryRow(`
			SELECT status FROM queued_jobs WHERE id = ?
		`, id).Scan(&status)

		if err != nil {
			t.Fatalf("Failed to query job: %v", err)
		}

		if status != string(queue.JobStatusPending) {
			t.Errorf("Expected job to still be pending after 500ms, got %s", status)
		}

		// Wait for job to be processed after delay
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("Job failed: %v", err)
			}

			err = db.QueryRow(`
				SELECT status FROM queued_jobs WHERE id = ?
			`, id).Scan(&status)

			if err != nil {
				t.Fatalf("Failed to query job: %v", err)
			}

			if status != string(queue.JobStatusCompleted) {
				t.Errorf("Expected status completed, got %s", status)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Timeout waiting for delayed job to be processed")
		}
	})
}

func TestWorker_StopGracefully(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		// Stop app's worker pool to avoid conflicts with test workers
		if server.App.QueueWorkerPool != nil {
			server.App.QueueWorkerPool.Stop()
		}

		systemDB := server.App.DatabaseManager.SystemDatabase()
		registry := queue.NewJobRegistry()

		// Register a job
		registry.Register("test-job", func(key string) (queue.Job, error) {
			return &TestJob{
				name:       "Test Job",
				key:        key,
				queueName:  "default",
				jobType:    "test-job",
				retries:    3,
				retryAfter: 5 * time.Second,
			}, nil
		})

		// Create and start worker
		worker := queue.NewWorker("worker-1", systemDB, registry)
		worker.Start()

		// Give worker time to start
		time.Sleep(100 * time.Millisecond)

		// Stop the worker
		worker.Stop()

		// Give worker time to stop
		time.Sleep(100 * time.Millisecond)

		// Worker should have stopped gracefully
		// If we got here without hanging, the test passes
	})
}
