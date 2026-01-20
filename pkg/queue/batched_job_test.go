package queue_test

import (
	"testing"
	"time"

	"github.com/litebase/litebase/pkg/queue"
)

func TestBatchStatus(t *testing.T) {
	tests := []struct {
		name   string
		status queue.BatchStatus
		want   string
	}{
		{"pending", queue.BatchStatusPending, "pending"},
		{"processing", queue.BatchStatusProcessing, "processing"},
		{"completed", queue.BatchStatusCompleted, "completed"},
		{"failed", queue.BatchStatusFailed, "failed"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if string(tt.status) != tt.want {
				t.Errorf("expected %s, got %s", tt.want, string(tt.status))
			}
		})
	}
}

func TestJobBatch(t *testing.T) {
	now := time.Now().UTC()
	finishedAt := now.Add(1 * time.Hour)

	batch := queue.JobBatch{
		ID:          1,
		Name:        "test-batch",
		TotalJobs:   10,
		PendingJobs: 5,
		FailedJobs:  2,
		FinishedAt:  &finishedAt,
		CreatedAt:   now,
	}

	if batch.ID != 1 {
		t.Errorf("expected ID 1, got %d", batch.ID)
	}

	if batch.Name != "test-batch" {
		t.Errorf("expected name 'test-batch', got %s", batch.Name)
	}

	if batch.TotalJobs != 10 {
		t.Errorf("expected 10 total jobs, got %d", batch.TotalJobs)
	}

	if batch.PendingJobs != 5 {
		t.Errorf("expected 5 pending jobs, got %d", batch.PendingJobs)
	}

	if batch.FailedJobs != 2 {
		t.Errorf("expected 2 failed jobs, got %d", batch.FailedJobs)
	}

	if batch.CreatedAt != now {
		t.Errorf("expected created at %v, got %v", now, batch.CreatedAt)
	}

	if batch.FinishedAt == nil {
		t.Error("expected finished at to be set")
	}

	if !batch.FinishedAt.Equal(finishedAt) {
		t.Errorf("expected finished at %v, got %v", finishedAt, batch.FinishedAt)
	}
}

func TestBatchedJob(t *testing.T) {
	now := time.Now().UTC()

	job := queue.BatchedJob{
		ID:        1,
		BatchID:   100,
		QueueID:   200,
		Status:    queue.BatchStatusProcessing,
		Progress:  50,
		CreatedAt: now,
		UpdatedAt: now,
	}

	if job.ID != 1 {
		t.Errorf("expected ID 1, got %d", job.ID)
	}

	if job.BatchID != 100 {
		t.Errorf("expected batch ID 100, got %d", job.BatchID)
	}

	if job.QueueID != 200 {
		t.Errorf("expected queue ID 200, got %d", job.QueueID)
	}

	if job.Status != queue.BatchStatusProcessing {
		t.Errorf("expected status %s, got %s", queue.BatchStatusProcessing, job.Status)
	}

	if job.Progress != 50 {
		t.Errorf("expected progress 50, got %d", job.Progress)
	}

	if job.CreatedAt != now {
		t.Errorf("expected created at %v, got %v", now, job.CreatedAt)
	}

	if job.UpdatedAt != now {
		t.Errorf("expected updated at %v, got %v", now, job.UpdatedAt)
	}
}

func TestBatchProgress(t *testing.T) {
	now := time.Now().UTC()
	finishedAt := now.Add(1 * time.Hour)

	progress := queue.BatchProgress{
		BatchID:       1,
		Name:          "test-batch",
		TotalJobs:     10,
		PendingJobs:   2,
		CompletedJobs: 7,
		FailedJobs:    1,
		Progress:      70,
		IsFinished:    true,
		CreatedAt:     now,
		FinishedAt:    &finishedAt,
	}

	if progress.BatchID != 1 {
		t.Errorf("expected batch ID 1, got %d", progress.BatchID)
	}

	if progress.Name != "test-batch" {
		t.Errorf("expected name 'test-batch', got %s", progress.Name)
	}

	if progress.TotalJobs != 10 {
		t.Errorf("expected 10 total jobs, got %d", progress.TotalJobs)
	}

	if progress.PendingJobs != 2 {
		t.Errorf("expected 2 pending jobs, got %d", progress.PendingJobs)
	}

	if progress.CompletedJobs != 7 {
		t.Errorf("expected 7 completed jobs, got %d", progress.CompletedJobs)
	}

	if progress.FailedJobs != 1 {
		t.Errorf("expected 1 failed job, got %d", progress.FailedJobs)
	}

	if progress.Progress != 70 {
		t.Errorf("expected 70%% progress, got %d%%", progress.Progress)
	}

	if !progress.IsFinished {
		t.Error("expected batch to be finished")
	}

	if progress.FinishedAt == nil {
		t.Error("expected finished at to be set")
	}

	if progress.CreatedAt != now {
		t.Errorf("expected created at %v, got %v", now, progress.CreatedAt)
	}

	// Test progress with no finished time
	progress2 := queue.BatchProgress{
		BatchID:       2,
		Name:          "active-batch",
		TotalJobs:     10,
		PendingJobs:   5,
		CompletedJobs: 5,
		FailedJobs:    0,
		Progress:      50,
		IsFinished:    false,
		CreatedAt:     now,
		FinishedAt:    nil,
	}

	if progress2.BatchID != 2 {
		t.Errorf("expected batch ID 2, got %d", progress2.BatchID)
	}

	if progress2.Name != "active-batch" {
		t.Errorf("expected name 'active-batch', got %s", progress2.Name)
	}

	if progress2.TotalJobs != 10 {
		t.Errorf("expected 10 total jobs, got %d", progress2.TotalJobs)
	}

	if progress2.PendingJobs != 5 {
		t.Errorf("expected 5 pending jobs, got %d", progress2.PendingJobs)
	}

	if progress2.CompletedJobs != 5 {
		t.Errorf("expected 5 completed jobs, got %d", progress2.CompletedJobs)
	}

	if progress2.FailedJobs != 0 {
		t.Errorf("expected 0 failed jobs, got %d", progress2.FailedJobs)
	}

	if progress2.Progress != 50 {
		t.Errorf("expected 50%% progress, got %d%%", progress2.Progress)
	}

	if progress2.IsFinished {
		t.Error("expected batch to not be finished")
	}

	if progress2.FinishedAt != nil {
		t.Error("expected finished at to be nil")
	}

	if progress2.CreatedAt != now {
		t.Errorf("expected created at %v, got %v", now, progress2.CreatedAt)
	}

}
