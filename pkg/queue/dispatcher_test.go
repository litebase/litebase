package queue_test

import (
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/queue"
)

// TestJob is a test implementation of the Job interface
type TestJob struct {
	name        string
	key         string
	queueName   string
	jobType     string
	retries     int
	retryAfter  time.Duration
	handleError error
}

func (j *TestJob) Handle() error {
	return j.handleError
}

func (j *TestJob) Name() string {
	if j.jobType == "" {
		return j.name
	}
	return j.jobType
}

func (j *TestJob) Key() string {
	return j.key
}
func (j *TestJob) QueueName() string {
	return j.queueName
}

func (j *TestJob) Retries() int {
	return j.retries
}

func (j *TestJob) RetryAfter() time.Duration {
	return j.retryAfter
}

func (j *TestJob) FromData(data map[string]any) error {
	if key, ok := data["key"].(string); ok {
		j.key = key
	}
	if handleError, ok := data["handleError"].(string); ok && handleError != "" {
		j.handleError = nil // We can't reconstruct the error from string in tests
	}
	return nil
}

func (j *TestJob) ToData() (map[string]any, error) {
	data := map[string]any{
		"key": j.key,
	}
	if j.handleError != nil {
		data["handleError"] = j.handleError.Error()
	}
	return data, nil
}

func (j *TestJob) NewInstance() any {
	return &TestJob{
		name:        j.name,
		key:         "",
		queueName:   j.queueName,
		jobType:     j.jobType,
		retries:     j.retries,
		retryAfter:  j.retryAfter,
		handleError: j.handleError,
	}
}

func TestDispatcher_Dispatch(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		systemDB := server.App.DatabaseManager.SystemDatabase()
		dispatcher := queue.NewDispatcher(systemDB)

		job := &TestJob{
			name:       "Test Job",
			key:        "test-job-1",
			queueName:  "default",
			retries:    3,
			retryAfter: 5 * time.Second,
		}

		// Dispatch the job
		id, err := dispatcher.Dispatch(job)

		if err != nil {
			t.Fatalf("Failed to dispatch job: %v", err)
		}

		if id <= 0 {
			t.Errorf("Expected positive job ID, got %d", id)
		}

		// Verify the job was stored correctly in the database
		db, err := systemDB.DB()

		if err != nil {
			t.Fatalf("Failed to get database: %v", err)
		}

		var queuedJob queue.QueuedJob

		err = db.QueryRow(`
			SELECT id, queue_name, name, key, status, attempts, max_attempts
			FROM queued_jobs WHERE id = ?
		`, id).Scan(
			&queuedJob.ID,
			&queuedJob.QueueName,
			&queuedJob.Name,
			&queuedJob.Key,
			&queuedJob.Status,
			&queuedJob.Attempts,
			&queuedJob.MaxAttempts,
		)

		if err != nil {
			t.Fatalf("Failed to query queued job: %v", err)
		}

		// Verify job fields
		if queuedJob.QueueName != job.QueueName() {
			t.Errorf("Expected queue_name %s, got %s", job.QueueName(), queuedJob.QueueName)
		}

		if queuedJob.Name != job.Name() {
			t.Errorf("Expected name %s, got %s", job.Name(), queuedJob.Name)
		}

		if queuedJob.Key != job.Key() {
			t.Errorf("Expected key %s, got %s", job.Key(), queuedJob.Key)
		}

		if queuedJob.Status != queue.JobStatusPending {
			t.Errorf("Expected status %s, got %s", queue.JobStatusPending, queuedJob.Status)
		}

		if queuedJob.Attempts != 0 {
			t.Errorf("Expected attempts 0, got %d", queuedJob.Attempts)
		}

		if queuedJob.MaxAttempts != job.Retries() {
			t.Errorf("Expected max_attempts %d, got %d", job.Retries(), queuedJob.MaxAttempts)
		}
	})
}

func TestDispatcher_DispatchWithDelay(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		systemDB := server.App.DatabaseManager.SystemDatabase()
		dispatcher := queue.NewDispatcher(systemDB)

		job := &TestJob{
			name:       "Delayed Job",
			key:        "delayed-job-1",
			queueName:  "default",
			retries:    3,
			retryAfter: 5 * time.Second,
		}

		delay := 10 * time.Minute

		// Dispatch the job with delay
		id, err := dispatcher.DispatchWithDelay(job, delay)
		if err != nil {
			t.Fatalf("Failed to dispatch job with delay: %v", err)
		}

		// Verify the job was stored with correct available_at time
		db, err := systemDB.DB()
		if err != nil {
			t.Fatalf("Failed to get database: %v", err)
		}

		var availableAtStr string
		err = db.QueryRow(`
			SELECT available_at FROM queued_jobs WHERE id = ?
		`, id).Scan(&availableAtStr)

		if err != nil {
			t.Fatalf("Failed to query available_at: %v", err)
		}

		availableAt, err := time.Parse(time.RFC3339, availableAtStr)
		if err != nil {
			t.Fatalf("Failed to parse available_at: %v", err)
		}

		// The available_at should be approximately now + delay
		expectedAvailableAt := time.Now().UTC().Add(delay)
		diff := availableAt.Sub(expectedAvailableAt).Abs()

		// Allow 2 seconds of tolerance for test execution time
		if diff > 2*time.Second {
			t.Errorf("available_at time mismatch. Expected ~%s, got %s (diff: %s)",
				expectedAvailableAt.Format(time.RFC3339),
				availableAt.Format(time.RFC3339),
				diff,
			)
		}
	})
}

func TestDispatcher_DispatchUnique(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		systemDB := server.App.DatabaseManager.SystemDatabase()
		dispatcher := queue.NewDispatcher(systemDB)

		job := &TestJob{
			name:       "Unique Job",
			key:        "unique-job-1",
			queueName:  "default",
			retries:    3,
			retryAfter: 5 * time.Second,
		}

		// First dispatch should create a new job
		id1, created1, err := dispatcher.DispatchUnique(job)
		if err != nil {
			t.Fatalf("Failed to dispatch unique job: %v", err)
		}

		if !created1 {
			t.Error("Expected first dispatch to create a new job")
		}

		if id1 <= 0 {
			t.Errorf("Expected positive job ID, got %d", id1)
		}

		// Second dispatch with same key should return existing job
		id2, created2, err := dispatcher.DispatchUnique(job)
		if err != nil {
			t.Fatalf("Failed to dispatch duplicate unique job: %v", err)
		}

		if created2 {
			t.Error("Expected second dispatch to not create a new job")
		}

		if id2 != id1 {
			t.Errorf("Expected same job ID %d, got %d", id1, id2)
		}

		// Verify only one job exists in the database
		db, err := systemDB.DB()
		if err != nil {
			t.Fatalf("Failed to get database: %v", err)
		}

		var count int
		err = db.QueryRow(`
			SELECT COUNT(*) FROM queued_jobs WHERE key = ?
		`, job.Key()).Scan(&count)

		if err != nil {
			t.Fatalf("Failed to count jobs: %v", err)
		}

		if count != 1 {
			t.Errorf("Expected 1 job with key %s, got %d", job.Key(), count)
		}
	})
}

func TestDispatcher_DispatchUnique_AllowsAfterCompletion(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		systemDB := server.App.DatabaseManager.SystemDatabase()
		dispatcher := queue.NewDispatcher(systemDB)

		job := &TestJob{
			name:       "Unique Job",
			key:        "unique-job-2",
			queueName:  "default",
			retries:    3,
			retryAfter: 5 * time.Second,
		}

		// First dispatch
		id1, created1, err := dispatcher.DispatchUnique(job)
		if err != nil {
			t.Fatalf("Failed to dispatch unique job: %v", err)
		}

		if !created1 {
			t.Error("Expected first dispatch to create a new job")
		}

		// Mark the job as completed
		db, err := systemDB.DB()
		if err != nil {
			t.Fatalf("Failed to get database: %v", err)
		}

		_, err = db.Exec(`
			UPDATE queued_jobs SET status = ? WHERE id = ?
		`, queue.JobStatusCompleted, id1)

		if err != nil {
			t.Fatalf("Failed to mark job as completed: %v", err)
		}

		// Dispatch again should create a new job since previous is completed
		id2, created2, err := dispatcher.DispatchUnique(job)
		if err != nil {
			t.Fatalf("Failed to dispatch unique job after completion: %v", err)
		}

		if !created2 {
			t.Error("Expected dispatch after completion to create a new job")
		}

		if id2 == id1 {
			t.Errorf("Expected different job ID, got same ID %d", id2)
		}

		// Verify two jobs exist (one completed, one pending)
		var count int
		err = db.QueryRow(`
			SELECT COUNT(*) FROM queued_jobs WHERE key = ?
		`, job.Key()).Scan(&count)

		if err != nil {
			t.Fatalf("Failed to count jobs: %v", err)
		}

		if count != 2 {
			t.Errorf("Expected 2 jobs with key %s, got %d", job.Key(), count)
		}
	})
}

func TestDispatcher_DispatchUniqueWithDelay(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		systemDB := server.App.DatabaseManager.SystemDatabase()
		dispatcher := queue.NewDispatcher(systemDB)

		job := &TestJob{
			name:       "Unique Delayed Job",
			key:        "unique-delayed-job-1",
			queueName:  "default",
			retries:    3,
			retryAfter: 5 * time.Second,
		}

		delay := 5 * time.Minute

		// First dispatch should create a new job with delay
		id1, created1, err := dispatcher.DispatchUniqueWithDelay(job, delay)
		if err != nil {
			t.Fatalf("Failed to dispatch unique job with delay: %v", err)
		}

		if !created1 {
			t.Error("Expected first dispatch to create a new job")
		}

		// Second dispatch should return existing job
		id2, created2, err := dispatcher.DispatchUniqueWithDelay(job, delay)
		if err != nil {
			t.Fatalf("Failed to dispatch duplicate unique job with delay: %v", err)
		}

		if created2 {
			t.Error("Expected second dispatch to not create a new job")
		}

		if id2 != id1 {
			t.Errorf("Expected same job ID %d, got %d", id1, id2)
		}

		// Verify the job has correct available_at
		db, err := systemDB.DB()
		if err != nil {
			t.Fatalf("Failed to get database: %v", err)
		}

		var availableAtStr string
		err = db.QueryRow(`
			SELECT available_at FROM queued_jobs WHERE id = ?
		`, id1).Scan(&availableAtStr)

		if err != nil {
			t.Fatalf("Failed to query available_at: %v", err)
		}

		availableAt, err := time.Parse(time.RFC3339, availableAtStr)
		if err != nil {
			t.Fatalf("Failed to parse available_at: %v", err)
		}

		expectedAvailableAt := time.Now().UTC().Add(delay)
		diff := availableAt.Sub(expectedAvailableAt).Abs()

		if diff > 2*time.Second {
			t.Errorf("available_at time mismatch. Expected ~%s, got %s (diff: %s)",
				expectedAvailableAt.Format(time.RFC3339),
				availableAt.Format(time.RFC3339),
				diff,
			)
		}
	})
}

func TestDispatcher_MultipleQueues(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		systemDB := server.App.DatabaseManager.SystemDatabase()
		dispatcher := queue.NewDispatcher(systemDB)

		// Dispatch jobs to different queues
		job1 := &TestJob{
			name:       "High Priority Job",
			key:        "job-1",
			queueName:  "high",
			retries:    3,
			retryAfter: 5 * time.Second,
		}

		job2 := &TestJob{
			name:       "Default Priority Job",
			key:        "job-2",
			queueName:  "default",
			retries:    3,
			retryAfter: 5 * time.Second,
		}

		job3 := &TestJob{
			name:       "Low Priority Job",
			key:        "job-3",
			queueName:  "low",
			retries:    3,
			retryAfter: 5 * time.Second,
		}

		_, err := dispatcher.Dispatch(job1)
		if err != nil {
			t.Fatalf("Failed to dispatch job1: %v", err)
		}

		_, err = dispatcher.Dispatch(job2)
		if err != nil {
			t.Fatalf("Failed to dispatch job2: %v", err)
		}

		_, err = dispatcher.Dispatch(job3)
		if err != nil {
			t.Fatalf("Failed to dispatch job3: %v", err)
		}

		// Verify jobs are in different queues
		db, err := systemDB.DB()
		if err != nil {
			t.Fatalf("Failed to get database: %v", err)
		}

		queues := []string{"high", "default", "low"}
		for _, queueName := range queues {
			var count int
			err = db.QueryRow(`
				SELECT COUNT(*) FROM queued_jobs WHERE queue_name = ?
			`, queueName).Scan(&count)

			if err != nil {
				t.Fatalf("Failed to count jobs in queue %s: %v", queueName, err)
			}

			if count != 1 {
				t.Errorf("Expected 1 job in queue %s, got %d", queueName, count)
			}
		}
	})
}

func TestDispatcher_DifferentJobTypes(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		systemDB := server.App.DatabaseManager.SystemDatabase()
		dispatcher := queue.NewDispatcher(systemDB)

		// Dispatch jobs of different types
		job1 := &TestJob{
			name:       "Email Job",
			key:        "email-1",
			jobType:    "email",
			queueName:  "default",
			retries:    5,
			retryAfter: 10 * time.Second,
		}

		job2 := &TestJob{
			name:       "Export Job",
			key:        "export-1",
			jobType:    "export",
			queueName:  "default",
			retries:    2,
			retryAfter: 30 * time.Second,
		}

		id1, err := dispatcher.Dispatch(job1)
		if err != nil {
			t.Fatalf("Failed to dispatch email job: %v", err)
		}

		id2, err := dispatcher.Dispatch(job2)
		if err != nil {
			t.Fatalf("Failed to dispatch export job: %v", err)
		}

		// Verify jobs have correct types and retry settings
		db, err := systemDB.DB()
		if err != nil {
			t.Fatalf("Failed to get database: %v", err)
		}

		var jobType1 string
		var maxAttempts1 int
		err = db.QueryRow(`
			SELECT name, max_attempts FROM queued_jobs WHERE id = ?
		`, id1).Scan(&jobType1, &maxAttempts1)

		if err != nil {
			t.Fatalf("Failed to query job1: %v", err)
		}

		if jobType1 != "email" {
			t.Errorf("Expected name email, got %s", jobType1)
		}

		if maxAttempts1 != 5 {
			t.Errorf("Expected max_attempts 5, got %d", maxAttempts1)
		}

		var jobType2 string
		var maxAttempts2 int
		err = db.QueryRow(`
			SELECT name, max_attempts FROM queued_jobs WHERE id = ?
		`, id2).Scan(&jobType2, &maxAttempts2)

		if err != nil {
			t.Fatalf("Failed to query job2: %v", err)
		}

		if jobType2 != "export" {
			t.Errorf("Expected name export, got %s", jobType2)
		}

		if maxAttempts2 != 2 {
			t.Errorf("Expected max_attempts 2, got %d", maxAttempts2)
		}
	})
}
