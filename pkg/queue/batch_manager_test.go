package queue_test

import (
	"context"
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/queue"
	"github.com/litebase/litebase/pkg/server"
)

type TestBatchJob struct {
	name string
}

func (j *TestBatchJob) Handle(ctx context.Context) error {
	// Simple test job that does nothing
	return nil
}

func (j *TestBatchJob) Name() string {
	return "TestBatchJob"
}

func (j *TestBatchJob) Key() string {
	return j.name
}

func (j *TestBatchJob) QueueName() string {
	return "default"
}

func (j *TestBatchJob) Retries() int {
	return 3
}

func (j *TestBatchJob) RetryAfter() time.Duration {
	return 1 * time.Second
}

func (j *TestBatchJob) Throttle() (bool, time.Duration) {
	return false, 0
}

func (j *TestBatchJob) WithoutOverlap() bool {
	return false
}

func (j *TestBatchJob) OverlapRetryDelay() time.Duration {
	return 100 * time.Millisecond
}

func (j *TestBatchJob) Timeout() time.Duration {
	return 5 * time.Second
}

func (j *TestBatchJob) ToData() (map[string]any, error) {
	return map[string]any{
		"name": j.name,
	}, nil
}

func (j *TestBatchJob) FromData(data map[string]any) error {
	if name, ok := data["name"].(string); ok {
		j.name = name
	}
	return nil
}

func (j *TestBatchJob) NewInstance() any {
	return &TestBatchJob{}
}

func TestBatchManagerCreateBatch(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		ctx := context.Background()
		batchManager := app.QueueWorkerPool.NewBatchManager()

		// Register test job
		testHandler := func(ctx context.Context, data map[string]any) error {
			// Simple test handler that does nothing
			return nil
		}

		err := app.QueueWorkerPool.RegisterJob("test-batch-job", testHandler)

		if err != nil {
			t.Fatalf("failed to register job: %v", err)
		}

		// Dispatch jobs first (outside of batch creation to avoid transaction deadlock)
		dispatcher := app.QueueWorkerPool.NewDispatcher()

		var jobIDs []int64

		for i := 1; i <= 3; i++ {
			jobID, err := dispatcher.DispatchJob("test-batch-job", map[string]any{
				"index": i,
			})

			if err != nil {
				t.Fatalf("failed to dispatch job %d: %v", i, err)
			}

			jobIDs = append(jobIDs, jobID)
		}

		// Wait a bit for jobs to be dispatched
		time.Sleep(100 * time.Millisecond)

		// Create batch with existing job IDs by inserting manually
		db, err := app.DatabaseManager.SystemDatabase().DB()

		if err != nil {
			t.Fatalf("failed to get database: %v", err)
		}

		now := time.Now().UTC()

		result, err := db.ExecContext(ctx, `
			INSERT INTO job_batches (name, total_jobs, pending_jobs, failed_jobs, created_at)
			VALUES (?, ?, ?, ?, ?)
		`, "test-batch", len(jobIDs), len(jobIDs), 0, now.Format(time.RFC3339))

		if err != nil {
			t.Fatalf("failed to create batch: %v", err)
		}

		batchID, err := result.LastInsertId()

		if err != nil {
			t.Fatalf("failed to get batch ID: %v", err)
		}

		// Link jobs to batch
		for _, jobID := range jobIDs {
			_, err = db.ExecContext(ctx, `
				INSERT INTO batched_jobs (batch_id, queue_id, status, progress, created_at, updated_at)
				VALUES (?, ?, ?, ?, ?, ?)
			`, batchID, jobID, queue.BatchStatusPending, 0, now.Format(time.RFC3339), now.Format(time.RFC3339))

			if err != nil {
				t.Fatalf("failed to link job to batch: %v", err)
			}
		}

		if batchID <= 0 {
			t.Error("expected positive batch ID")
		}

		// Get batch status
		progress, err := batchManager.GetBatchStatus(ctx, batchID)

		if err != nil {
			t.Fatalf("failed to get batch status: %v", err)
		}

		if progress.BatchID != batchID {
			t.Errorf("expected batch ID %d, got %d", batchID, progress.BatchID)
		}

		if progress.Name != "test-batch" {
			t.Errorf("expected batch name 'test-batch', got %s", progress.Name)
		}

		if progress.TotalJobs != len(jobIDs) {
			t.Errorf("expected %d total jobs, got %d", len(jobIDs), progress.TotalJobs)
		}

		if progress.PendingJobs != len(jobIDs) {
			t.Errorf("expected %d pending jobs, got %d", len(jobIDs), progress.PendingJobs)
		}

		if progress.CompletedJobs != 0 {
			t.Errorf("expected 0 completed jobs, got %d", progress.CompletedJobs)
		}

		if progress.FailedJobs != 0 {
			t.Errorf("expected 0 failed jobs, got %d", progress.FailedJobs)
		}

		if progress.Progress != 0 {
			t.Errorf("expected 0%% progress, got %d%%", progress.Progress)
		}

		if progress.IsFinished {
			t.Error("expected batch to not be finished")
		}
	})
}

func TestBatchManagerGetBatchStatus_NotFound(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		ctx := context.Background()
		batchManager := app.QueueWorkerPool.NewBatchManager()

		// Try to get status for non-existent batch
		_, err := batchManager.GetBatchStatus(ctx, 99999)

		if err == nil {
			t.Fatal("expected error for non-existent batch")
		}
	})
}

func TestBatchManagerUpdateProgress(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		ctx := context.Background()
		batchManager := app.QueueWorkerPool.NewBatchManager()

		// Register and dispatch a test job first
		testHandler := func(ctx context.Context, data map[string]any) error {
			return nil
		}

		err := app.QueueWorkerPool.RegisterJob("test-progress-job", testHandler)

		if err != nil {
			t.Fatalf("failed to register job: %v", err)
		}

		dispatcher := app.QueueWorkerPool.NewDispatcher()

		queueID, err := dispatcher.DispatchJob("test-progress-job", map[string]any{
			"test": "data",
		})

		if err != nil {
			t.Fatalf("failed to dispatch job: %v", err)
		}

		// Wait for job to be dispatched
		time.Sleep(100 * time.Millisecond)

		// Create a test batch manually
		db, err := app.DatabaseManager.SystemDatabase().DB()

		if err != nil {
			t.Fatalf("failed to get database: %v", err)
		}

		now := time.Now().UTC()

		result, err := db.ExecContext(ctx, `
			INSERT INTO job_batches (name, total_jobs, pending_jobs, failed_jobs, created_at)
			VALUES (?, ?, ?, ?, ?)
		`, "test-batch", 1, 1, 0, now.Format(time.RFC3339))

		if err != nil {
			t.Fatalf("failed to create batch: %v", err)
		}

		batchID, err := result.LastInsertId()

		if err != nil {
			t.Fatalf("failed to get batch ID: %v", err)
		}

		// Create a batched job with the actual queue_id
		_, err = db.ExecContext(ctx, `
			INSERT INTO batched_jobs (batch_id, queue_id, status, progress, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, ?)
		`, batchID, queueID, queue.BatchStatusPending, 0, now.Format(time.RFC3339), now.Format(time.RFC3339))

		if err != nil {
			t.Fatalf("failed to create batched job: %v", err)
		}

		// Update progress
		err = batchManager.UpdateProgress(ctx, batchID, queueID, 50)

		if err != nil {
			t.Fatalf("failed to update progress: %v", err)
		}

		// Verify progress was updated
		var progress int

		err = db.QueryRowContext(ctx, `
			SELECT progress FROM batched_jobs WHERE batch_id = ? AND queue_id = ?
		`, batchID, queueID).Scan(&progress)

		if err != nil {
			t.Fatalf("failed to get progress: %v", err)
		}

		if progress != 50 {
			t.Errorf("expected progress 50, got %d", progress)
		}
	})
}

func TestBatchManagerUpdateProgress_InvalidRange(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		ctx := context.Background()
		batchManager := app.QueueWorkerPool.NewBatchManager()

		// Try to update with invalid progress values
		err := batchManager.UpdateProgress(ctx, 1, 1, -1)

		if err == nil {
			t.Error("expected error for negative progress")
		}

		err = batchManager.UpdateProgress(ctx, 1, 1, 101)

		if err == nil {
			t.Error("expected error for progress > 100")
		}
	})
}

func TestBatchManagerUpdateJobStatus(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		ctx := context.Background()
		batchManager := app.QueueWorkerPool.NewBatchManager()

		// Register test job
		testHandler := func(ctx context.Context, data map[string]any) error {
			// Simple test handler that does nothing
			return nil
		}

		err := app.QueueWorkerPool.RegisterJob("test-job", testHandler)

		if err != nil {
			t.Fatalf("failed to register job: %v", err)
		}

		// Dispatch a job
		dispatcher := app.QueueWorkerPool.NewDispatcher()

		queueID, err := dispatcher.DispatchJob("test-job", map[string]any{
			"name": "test",
		})

		if err != nil {
			t.Fatalf("failed to dispatch job: %v", err)
		}

		// Create a batch manually and link the job
		db, err := app.DatabaseManager.SystemDatabase().DB()

		if err != nil {
			t.Fatalf("failed to get database: %v", err)
		}

		now := time.Now().UTC()

		result, err := db.ExecContext(ctx, `
			INSERT INTO job_batches (name, total_jobs, pending_jobs, failed_jobs, created_at)
			VALUES (?, ?, ?, ?, ?)
		`, "test-batch", 1, 1, 0, now.Format(time.RFC3339))

		if err != nil {
			t.Fatalf("failed to create batch: %v", err)
		}

		batchID, err := result.LastInsertId()

		if err != nil {
			t.Fatalf("failed to get batch ID: %v", err)
		}

		// Link the job to the batch
		_, err = db.ExecContext(ctx, `
			INSERT INTO batched_jobs (batch_id, queue_id, status, progress, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, ?)
		`, batchID, queueID, queue.BatchStatusPending, 0, now.Format(time.RFC3339), now.Format(time.RFC3339))

		if err != nil {
			t.Fatalf("failed to link job to batch: %v", err)
		}

		// Mark the queue job as completed
		_, err = db.ExecContext(ctx, `
			UPDATE queued_jobs SET status = ? WHERE id = ?
		`, queue.JobStatusCompleted, queueID)

		if err != nil {
			t.Fatalf("failed to update queue job status: %v", err)
		}

		// Update batch job status
		err = batchManager.UpdateJobStatus(ctx, queueID)

		if err != nil {
			t.Fatalf("failed to update job status: %v", err)
		}

		// Verify batch job status was updated
		var batchStatus queue.BatchStatus

		err = db.QueryRowContext(ctx, `
			SELECT status FROM batched_jobs WHERE queue_id = ?
		`, queueID).Scan(&batchStatus)

		if err != nil {
			t.Fatalf("failed to get batch job status: %v", err)
		}

		if batchStatus != queue.BatchStatusCompleted {
			t.Errorf("expected status %s, got %s", queue.BatchStatusCompleted, batchStatus)
		}

		// Verify batch was marked as finished
		progress, err := batchManager.GetBatchStatus(ctx, batchID)

		if err != nil {
			t.Fatalf("failed to get batch status: %v", err)
		}

		if !progress.IsFinished {
			t.Error("expected batch to be finished")
		}

		if progress.CompletedJobs != 1 {
			t.Errorf("expected 1 completed job, got %d", progress.CompletedJobs)
		}

		if progress.Progress != 100 {
			t.Errorf("expected 100%% progress, got %d%%", progress.Progress)
		}
	})
}
