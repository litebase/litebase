package queue

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"github.com/litebase/litebase/pkg/database"
)

type BatchManager struct {
	systemDB   *database.SystemDatabase
	dispatcher *Dispatcher
}

// Create a new BatchManager
func NewBatchManager(systemDB *database.SystemDatabase) *BatchManager {
	return &BatchManager{
		systemDB: systemDB,
	}
}

// Create a new job batch with the given name and jobs.
func (bm *BatchManager) CreateBatch(ctx context.Context, name string, jobs []Job) (int64, error) {
	if bm.dispatcher == nil {
		return 0, fmt.Errorf("dispatcher not set")
	}

	db, err := bm.systemDB.DB()

	if err != nil {
		return 0, fmt.Errorf("failed to get database: %w", err)
	}

	tx, err := db.BeginTx(ctx, nil)

	if err != nil {
		return 0, fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer func() {
		if err := tx.Rollback(); err != nil && err != sql.ErrTxDone {
			slog.Error("Failed to rollback transaction", "error", err)
		}
	}()

	now := time.Now().UTC()

	result, err := tx.ExecContext(ctx, `INSERT INTO job_batches (name, total_jobs, pending_jobs, failed_jobs, created_at) VALUES (?, ?, ?, ?, ?)`, name, len(jobs), len(jobs), 0, now.Format(time.RFC3339))

	if err != nil {
		return 0, fmt.Errorf("failed to create batch: %w", err)
	}

	batchID, err := result.LastInsertId()

	if err != nil {
		return 0, fmt.Errorf("failed to get batch ID: %w", err)
	}

	for _, job := range jobs {
		queueID, err := bm.dispatcher.Dispatch(job)

		if err != nil {
			return 0, fmt.Errorf("failed to dispatch job: %w", err)
		}

		_, err = tx.ExecContext(ctx, `INSERT INTO batched_jobs (batch_id, queue_id, status, progress, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)`, batchID, queueID, BatchStatusPending, 0, now.Format(time.RFC3339), now.Format(time.RFC3339))

		if err != nil {
			return 0, fmt.Errorf("failed to create batched job record: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("failed to commit transaction: %w", err)
	}

	slog.Info("Created job batch", "batch_id", batchID, "name", name, "total_jobs", len(jobs))

	return batchID, nil
}

// Update the progress of a job within a batch.
func (bm *BatchManager) UpdateProgress(ctx context.Context, batchID int64, queueID int64, progress int) error {
	if progress < 0 || progress > 100 {
		return fmt.Errorf("progress must be between 0 and 100")
	}

	db, err := bm.systemDB.DB()

	if err != nil {
		return fmt.Errorf("failed to get database: %w", err)
	}

	now := time.Now().UTC()

	_, err = db.ExecContext(ctx, `UPDATE batched_jobs SET progress = ?, updated_at = ? WHERE batch_id = ? AND queue_id = ?`, progress, now.Format(time.RFC3339), batchID, queueID)

	if err != nil {
		return fmt.Errorf("failed to update job progress: %w", err)
	}

	return nil
}

// UpdateJobStatus updates the status of a job in a batch based on the status of the corresponding queue job.
func (bm *BatchManager) UpdateJobStatus(ctx context.Context, queueID int64) error {
	db, err := bm.systemDB.DB()

	if err != nil {
		return fmt.Errorf("failed to get database: %w", err)
	}

	var queueStatus JobStatus

	err = db.QueryRowContext(ctx, `SELECT status FROM queued_jobs WHERE id = ?`, queueID).Scan(&queueStatus)

	if err != nil {
		return fmt.Errorf("failed to get queue job status: %w", err)
	}

	var batchStatus BatchStatus
	switch queueStatus {
	case JobStatusPending:
		batchStatus = BatchStatusPending
	case JobStatusReserved:
		batchStatus = BatchStatusProcessing
	case JobStatusCompleted:
		batchStatus = BatchStatusCompleted
	case JobStatusFailed:
		batchStatus = BatchStatusFailed
	default:
		batchStatus = BatchStatusPending
	}

	tx, err := db.BeginTx(ctx, nil)

	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer func() {
		if err := tx.Rollback(); err != nil && err != sql.ErrTxDone {
			slog.Error("Failed to rollback transaction", "error", err)
		}
	}()

	var batchID int64
	var oldStatus BatchStatus

	err = tx.QueryRowContext(ctx, `SELECT batch_id, status FROM batched_jobs WHERE queue_id = ?`, queueID).Scan(&batchID, &oldStatus)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil
		}

		return fmt.Errorf("failed to get batch ID: %w", err)
	}

	now := time.Now().UTC()

	_, err = tx.ExecContext(ctx, `UPDATE batched_jobs SET status = ?, updated_at = ? WHERE queue_id = ?`, batchStatus, now.Format(time.RFC3339), queueID)

	if err != nil {
		return fmt.Errorf("failed to update batched job status: %w", err)
	}

	if oldStatus != batchStatus {
		if oldStatus == BatchStatusPending {
			_, err = tx.ExecContext(ctx, `UPDATE job_batches SET pending_jobs = pending_jobs - 1 WHERE id = ?`, batchID)

			if err != nil {
				return fmt.Errorf("failed to decrement pending jobs: %w", err)
			}
		}

		if batchStatus == BatchStatusFailed {
			_, err = tx.ExecContext(ctx, `UPDATE job_batches SET failed_jobs = failed_jobs + 1 WHERE id = ?`, batchID)

			if err != nil {
				return fmt.Errorf("failed to increment failed jobs: %w", err)
			}
		}

		if batchStatus == BatchStatusCompleted || batchStatus == BatchStatusFailed {
			var pendingJobs int

			err = tx.QueryRowContext(ctx, `SELECT pending_jobs FROM job_batches WHERE id = ?`, batchID).Scan(&pendingJobs)

			if err != nil {
				return fmt.Errorf("failed to get pending jobs count: %w", err)
			}

			if pendingJobs == 0 {
				_, err = tx.ExecContext(ctx, `UPDATE job_batches SET finished_at = ? WHERE id = ?`, now.Format(time.RFC3339), batchID)

				if err != nil {
					return fmt.Errorf("failed to mark batch as finished: %w", err)
				}

				slog.Info("Batch completed", "batch_id", batchID)
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// GetBatchStatus retrieves the status of a job batch.
func (bm *BatchManager) GetBatchStatus(ctx context.Context, batchID int64) (*BatchProgress, error) {
	db, err := bm.systemDB.DB()

	if err != nil {
		return nil, fmt.Errorf("failed to get database: %w", err)
	}

	var batch JobBatch
	var finishedAtStr sql.NullString

	err = db.QueryRowContext(ctx, `SELECT id, name, total_jobs, pending_jobs, failed_jobs, created_at, finished_at FROM job_batches WHERE id = ?`, batchID).Scan(&batch.ID, &batch.Name, &batch.TotalJobs, &batch.PendingJobs, &batch.FailedJobs, &batch.CreatedAt, &finishedAtStr)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("batch not found")
		}

		return nil, fmt.Errorf("failed to get batch: %w", err)
	}

	var finishedAt *time.Time

	if finishedAtStr.Valid {
		parsed, err := time.Parse(time.RFC3339, finishedAtStr.String)

		if err == nil {
			finishedAt = &parsed
		}
	}

	completedJobs := batch.TotalJobs - batch.PendingJobs - batch.FailedJobs

	var progress int

	if batch.TotalJobs > 0 {
		progress = (completedJobs * 100) / batch.TotalJobs
	}

	return &BatchProgress{
		BatchID:       batch.ID,
		Name:          batch.Name,
		TotalJobs:     batch.TotalJobs,
		PendingJobs:   batch.PendingJobs,
		CompletedJobs: completedJobs,
		FailedJobs:    batch.FailedJobs,
		Progress:      progress,
		IsFinished:    finishedAt != nil,
		CreatedAt:     batch.CreatedAt,
		FinishedAt:    finishedAt,
	}, nil
}
