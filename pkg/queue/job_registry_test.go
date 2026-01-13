package queue_test

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/litebase/litebase/pkg/queue"
)

// TestJob is defined in worker_test.go

func TestJobRegistry_NewJobRegistry(t *testing.T) {
	registry := queue.NewJobRegistry()

	if registry == nil {
		t.Fatal("Expected NewJobRegistry to return a non-nil registry")
	}
}

func TestJobRegistry_Register(t *testing.T) {
	registry := queue.NewJobRegistry()

	// Register a job factory
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

	// Should be able to get the registered job
	job, err := registry.Get("test-job", "test-key-1")

	if err != nil {
		t.Fatalf("Expected no error getting registered job, got: %v", err)
	}

	if job == nil {
		t.Fatal("Expected job to be non-nil")
	}

	if job.JobType() != "test-job" {
		t.Errorf("Expected job type 'test-job', got '%s'", job.JobType())
	}

	if job.Key() != "test-key-1" {
		t.Errorf("Expected job key 'test-key-1', got '%s'", job.Key())
	}
}

func TestJobRegistry_GetUnregisteredJobType(t *testing.T) {
	registry := queue.NewJobRegistry()

	// Try to get an unregistered job type
	job, err := registry.Get("unregistered-job", "test-key-1")

	if err == nil {
		t.Fatal("Expected error when getting unregistered job type")
	}

	if job != nil {
		t.Error("Expected job to be nil for unregistered type")
	}

	expectedError := "job type unregistered-job not registered"

	if err.Error() != expectedError {
		t.Errorf("Expected error message '%s', got '%s'", expectedError, err.Error())
	}
}

func TestJobRegistry_RegisterMultipleJobTypes(t *testing.T) {
	registry := queue.NewJobRegistry()

	// Register multiple job types
	registry.Register("email-job", func(key string) (queue.Job, error) {
		return &TestJob{
			name:       "Email Job",
			key:        key,
			queueName:  "emails",
			jobType:    "email-job",
			retries:    5,
			retryAfter: 10 * time.Second,
		}, nil
	})

	registry.Register("export-job", func(key string) (queue.Job, error) {
		return &TestJob{
			name:       "Export Job",
			key:        key,
			queueName:  "exports",
			jobType:    "export-job",
			retries:    2,
			retryAfter: 30 * time.Second,
		}, nil
	})

	// Get email job
	emailJob, err := registry.Get("email-job", "email-1")
	if err != nil {
		t.Fatalf("Expected no error getting email job, got: %v", err)
	}

	if emailJob.JobType() != "email-job" {
		t.Errorf("Expected job type 'email-job', got '%s'", emailJob.JobType())
	}

	if emailJob.QueueName() != "emails" {
		t.Errorf("Expected queue name 'emails', got '%s'", emailJob.QueueName())
	}

	if emailJob.Retries() != 5 {
		t.Errorf("Expected 5 retries, got %d", emailJob.Retries())
	}

	// Get export job
	exportJob, err := registry.Get("export-job", "export-1")
	if err != nil {
		t.Fatalf("Expected no error getting export job, got: %v", err)
	}

	if exportJob.JobType() != "export-job" {
		t.Errorf("Expected job type 'export-job', got '%s'", exportJob.JobType())
	}

	if exportJob.QueueName() != "exports" {
		t.Errorf("Expected queue name 'exports', got '%s'", exportJob.QueueName())
	}

	if exportJob.Retries() != 2 {
		t.Errorf("Expected 2 retries, got %d", exportJob.Retries())
	}
}

func TestJobRegistry_FactoryReturnsError(t *testing.T) {
	registry := queue.NewJobRegistry()

	// Register a factory that returns an error
	registry.Register("error-job", func(key string) (queue.Job, error) {
		return nil, errors.New("factory error")
	})

	job, err := registry.Get("error-job", "test-key-1")

	if err == nil {
		t.Fatal("Expected error from factory")
	}

	if job != nil {
		t.Error("Expected job to be nil when factory returns error")
	}

	if err.Error() != "factory error" {
		t.Errorf("Expected error 'factory error', got '%s'", err.Error())
	}
}

func TestJobRegistry_ConcurrentRegister(t *testing.T) {
	registry := queue.NewJobRegistry()

	// Register multiple job types concurrently
	var wg sync.WaitGroup
	numJobs := 100

	for i := 0; i < numJobs; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()

			jobType := "job-" + string(rune('A'+index%26))
			registry.Register(jobType, func(key string) (queue.Job, error) {
				return &TestJob{
					name:       "Job " + jobType,
					key:        key,
					queueName:  "default",
					jobType:    jobType,
					retries:    3,
					retryAfter: 5 * time.Second,
				}, nil
			})
		}(i)
	}

	wg.Wait()

	// Verify we can get all registered jobs
	for i := range 26 {
		jobType := "job-" + string(rune('A'+i))
		job, err := registry.Get(jobType, "test-key")

		if err != nil {
			t.Errorf("Expected no error getting job type '%s', got: %v", jobType, err)
		}

		if job == nil {
			t.Errorf("Expected job for type '%s' to be non-nil", jobType)
		}
	}
}

func TestJobRegistry_ConcurrentGet(t *testing.T) {
	registry := queue.NewJobRegistry()

	// Register a job type
	registry.Register("concurrent-job", func(key string) (queue.Job, error) {
		return &TestJob{
			name:       "Concurrent Job",
			key:        key,
			queueName:  "default",
			jobType:    "concurrent-job",
			retries:    3,
			retryAfter: 5 * time.Second,
		}, nil
	})

	// Get the job concurrently many times
	var wg sync.WaitGroup
	numGets := 100
	errorsChan := make(chan error, numGets)

	for i := range numGets {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()

			job, err := registry.Get("concurrent-job", "key-"+string(rune('0'+index%10)))

			if err != nil {
				errorsChan <- err
				return
			}

			if job == nil {
				errorsChan <- errors.New("job is nil")
			}
		}(i)
	}

	wg.Wait()
	close(errorsChan)

	// Check for any errors
	for err := range errorsChan {
		t.Errorf("Concurrent get error: %v", err)
	}
}

func TestJobRegistry_ConcurrentRegisterAndGet(t *testing.T) {
	registry := queue.NewJobRegistry()

	var wg sync.WaitGroup

	// Concurrently register jobs
	for i := range 10 {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()

			jobType := "job-" + string(rune('A'+index))
			registry.Register(jobType, func(key string) (queue.Job, error) {
				return &TestJob{
					name:       "Job " + jobType,
					key:        key,
					queueName:  "default",
					jobType:    jobType,
					retries:    3,
					retryAfter: 5 * time.Second,
				}, nil
			})
		}(i)
	}

	// Concurrently get jobs (some may not exist yet)
	for i := range 20 {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()

			jobType := "job-" + string(rune('A'+index%10))
			_, _ = registry.Get(jobType, "test-key")
			// We don't check errors here because the job might not be registered yet
		}(i)
	}

	wg.Wait()

	// Verify all jobs are registered
	for i := range 10 {
		jobType := "job-" + string(rune('A'+i))
		job, err := registry.Get(jobType, "test-key")

		if err != nil {
			t.Errorf("Expected no error getting job type '%s', got: %v", jobType, err)
		}

		if job == nil {
			t.Errorf("Expected job for type '%s' to be non-nil", jobType)
		}
	}
}

func TestJobRegistry_OverwriteRegistration(t *testing.T) {
	registry := queue.NewJobRegistry()

	// Register a job type
	registry.Register("overwrite-job", func(key string) (queue.Job, error) {
		return &TestJob{
			name:       "Original Job",
			key:        key,
			queueName:  "original",
			jobType:    "overwrite-job",
			retries:    3,
			retryAfter: 5 * time.Second,
		}, nil
	})

	// Overwrite with a new factory
	registry.Register("overwrite-job", func(key string) (queue.Job, error) {
		return &TestJob{
			name:       "Updated Job",
			key:        key,
			queueName:  "updated",
			jobType:    "overwrite-job",
			retries:    5,
			retryAfter: 10 * time.Second,
		}, nil
	})

	// Get the job and verify it uses the updated factory
	job, err := registry.Get("overwrite-job", "test-key")

	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	if job.Name() != "Updated Job" {
		t.Errorf("Expected name 'Updated Job', got '%s'", job.Name())
	}

	if job.QueueName() != "updated" {
		t.Errorf("Expected queue name 'updated', got '%s'", job.QueueName())
	}

	if job.Retries() != 5 {
		t.Errorf("Expected 5 retries, got %d", job.Retries())
	}
}
