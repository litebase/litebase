package queue_test

import (
	"errors"
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/queue"
)

// TestRetryJob is a special job that tracks attempts for retry testing
type TestRetryJob struct {
	name             string
	key              string
	queueName        string
	jobType          string
	retries          int
	retryAfter       time.Duration
	attemptCount     *int
	failUntilAttempt int
}

func (j *TestRetryJob) Handle() error {
	*j.attemptCount++
	if *j.attemptCount < j.failUntilAttempt {
		return errors.New("temporary failure")
	}
	return nil
}

func (j *TestRetryJob) JobType() string {
	return j.jobType
}

func (j *TestRetryJob) Key() string {
	return j.key
}

func (j *TestRetryJob) Name() string {
	return j.name
}

func (j *TestRetryJob) QueueName() string {
	return j.queueName
}

func (j *TestRetryJob) Retries() int {
	return j.retries
}

func (j *TestRetryJob) RetryAfter() time.Duration {
	return j.retryAfter
}

func (j *TestRetryJob) Throttle() (shouldThrottle bool, delay time.Duration) {
	return false, 0
}

func (j *TestRetryJob) FromData(data map[string]any) error {
	if key, ok := data["key"].(string); ok {
		j.key = key
	}
	return nil
}

func (j *TestRetryJob) ToData() (map[string]any, error) {
	return map[string]any{
		"key": j.key,
	}, nil
}

func (j *TestRetryJob) NewInstance() any {
	return &TestRetryJob{
		name:             j.name,
		key:              "",
		queueName:        j.queueName,
		jobType:          j.jobType,
		retries:          j.retries,
		retryAfter:       j.retryAfter,
		attemptCount:     j.attemptCount,
		failUntilAttempt: j.failUntilAttempt,
	}
}

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
		registry.Register(&TestJob{
			name:        "Test Job",
			key:         "",
			queueName:   "default",
			jobType:     "test-job",
			retries:     3,
			retryAfter:  5 * time.Second,
			handleError: nil, // Success
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
		registry.Register(&TestJob{
			name:        "Failing Job",
			key:         "",
			queueName:   "default",
			jobType:     "failing-job",
			retries:     0, // No retries
			retryAfter:  5 * time.Second,
			handleError: errors.New("job failed"),
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
		// Note: With the new design, we register the prototype and the error is static
		// For this test, we'll use a job that always fails temporarily
		registry.Register(&TestRetryJob{
			name:             "Retry Job",
			key:              "",
			queueName:        "default",
			jobType:          "retry-job",
			retries:          3,
			retryAfter:       500 * time.Millisecond,
			attemptCount:     &attemptCount,
			failUntilAttempt: 3,
		})

		// Dispatch a job
		job := &TestRetryJob{
			name:             "Retry Job",
			key:              "retry-key-1",
			queueName:        "default",
			jobType:          "retry-job",
			retries:          3,
			retryAfter:       500 * time.Millisecond,
			attemptCount:     &attemptCount,
			failUntilAttempt: 3,
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
		registry.Register(&TestJob{
			name:        "Always Fail Job",
			key:         "",
			queueName:   "default",
			jobType:     "always-fail-job",
			retries:     2,
			retryAfter:  200 * time.Millisecond,
			handleError: errors.New("persistent failure"),
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
		registry.Register(&TestJob{
			name:       "Delayed Job",
			key:        "",
			queueName:  "default",
			jobType:    "delayed-job",
			retries:    3,
			retryAfter: 5 * time.Second,
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
		registry.Register(&TestJob{
			name:       "Test Job",
			key:        "",
			queueName:  "default",
			jobType:    "test-job",
			retries:    3,
			retryAfter: 5 * time.Second,
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

func TestWorker_ReplicaPromotedToPrimaryProcessesJobs(t *testing.T) {
	test.Run(t, func() {
		// Start primary server
		server1 := test.NewTestServer(t)

		if !server1.App.Cluster.Node().IsPrimary() {
			t.Fatal("Server1 should be primary")
		}

		// Start replica server
		server2 := test.NewTestServer(t)
		defer server2.Shutdown()

		if !server2.App.Cluster.Node().IsReplica() {
			t.Fatal("Server2 should be replica")
		}

		// Create worker pool with primary-only mode on both servers
		// This simulates the real-world scenario where worker pools are configured
		// to only process jobs when the node is primary
		systemDB1 := server1.App.DatabaseManager.SystemDatabase()
		systemDB2 := server2.App.DatabaseManager.SystemDatabase()

		// Stop the default worker pools that the servers start
		if server1.App.QueueWorkerPool != nil {
			server1.App.QueueWorkerPool.Stop()
		}

		if server2.App.QueueWorkerPool != nil {
			server2.App.QueueWorkerPool.Stop()
		}

		// Create worker pools for both servers with primary-only mode
		pool1 := queue.NewWorkerPool(systemDB1, server1.App.Cluster, queue.WorkerPoolConfig{
			WorkerCount: 1,
			PrimaryOnly: true,
		})

		pool2 := queue.NewWorkerPool(systemDB2, server2.App.Cluster, queue.WorkerPoolConfig{
			WorkerCount: 1,
			PrimaryOnly: true,
		})

		// Register a test job on both pools
		jobProcessed := make(chan bool, 1)

		// Create a simple handler that succeeds
		testHandler := func(data map[string]any) error {
			return nil // Success
		}

		err := pool1.RegisterJob("TestJob", testHandler, queue.WithQueue("default"), queue.WithRetries(3, 5*time.Second))

		if err != nil {
			t.Fatalf("Failed to register job on pool1: %v", err)
		}

		err = pool2.RegisterJob("TestJob", testHandler, queue.WithQueue("default"), queue.WithRetries(3, 5*time.Second))

		if err != nil {
			t.Fatalf("Failed to register job on pool2: %v", err)
		}

		// Start both pools
		if err := pool1.Start(); err != nil {
			t.Fatalf("Failed to start pool1: %v", err)
		}

		if err := pool2.Start(); err != nil {
			t.Fatalf("Failed to start pool2: %v", err)
		}

		// Dispatch a job on the primary
		dispatcher := server1.App.QueueDispatcher

		jobID, err := dispatcher.DispatchJob("TestJob", map[string]any{
			"key": "test-key-1",
		})

		if err != nil {
			t.Fatalf("Failed to dispatch job: %v", err)
		}

		// Stop the primary's worker pool
		pool1.Stop()

		// Give time for the pool to stop
		time.Sleep(100 * time.Millisecond)

		// Shutdown the primary server
		server1.Shutdown()

		// Wait for server2 to detect that server1 is gone and become primary
		timeout := time.After(10 * time.Second)
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()

		becamePrimary := false

		for !becamePrimary {
			select {
			case <-timeout:
				t.Fatal("Timeout waiting for server2 to become primary")
			case <-ticker.C:
				if server2.App.Cluster.Node().IsPrimary() {
					becamePrimary = true
				}
			}
		}

		// Server2 is now primary - verify it processes the job
		// We need to check the database since the job was dispatched before server2 became primary
		jobTimeout := time.After(5 * time.Second)
		jobTicker := time.NewTicker(100 * time.Millisecond)
		defer jobTicker.Stop()

		jobCompleted := false

		for !jobCompleted {
			select {
			case <-jobTimeout:
				t.Fatal("Timeout waiting for job to be processed by new primary")
			case <-jobTicker.C:
				db, err := systemDB2.DB()

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

				if status == string(queue.JobStatusCompleted) {
					jobCompleted = true
					jobProcessed <- true
				}
			}
		}

		// Verify the job was processed
		select {
		case <-jobProcessed:
			// Success!
		case <-time.After(1 * time.Second):
			t.Fatal("Job was not marked as processed")
		}

		// Clean up
		pool2.Stop()
	})
}
