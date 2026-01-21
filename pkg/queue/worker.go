package queue

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/litebase/litebase/pkg/database"
)

// Worker processes jobs from the queue.
type Worker struct {
	id               string
	systemDB         *database.SystemDatabase
	ctx              context.Context
	cancel           context.CancelFunc
	pollInterval     time.Duration
	registry         *JobRegistry
	batchManager     *BatchManager
	wg               sync.WaitGroup
	afterJob         func(jobID int64, status JobStatus, err error)
	primaryOnly      bool
	isPrimary        func() bool
	runningJobKeys   *sync.Map // Shared map to track running job keys across all workers
	reservationMutex *sync.Mutex // Shared mutex to serialize job reservation and prevent DB locks
}

// NewWorker creates a new worker instance.
func NewWorker(id string, systemDB *database.SystemDatabase, registry *JobRegistry) *Worker {
	ctx, cancel := context.WithCancel(context.Background())

	return &Worker{
		id:           id,
		systemDB:     systemDB,
		ctx:          ctx,
		cancel:       cancel,
		pollInterval: 1 * time.Second,
		registry:     registry,
	}
}

// SetAfterJobHook sets a callback to be called after each job is processed.
// This is useful for testing to wait for job completion synchronously.
func (w *Worker) SetAfterJobHook(fn func(jobID int64, status JobStatus, err error)) {
	w.afterJob = fn
}

// SetBatchManager sets the batch manager for this worker.
func (w *Worker) SetBatchManager(batchManager *BatchManager) {
	w.batchManager = batchManager
}

// SetPrimaryOnlyMode configures the worker to only process jobs when on a primary node.
func (w *Worker) SetPrimaryOnlyMode(primaryOnly bool, isPrimary func() bool) {
	w.primaryOnly = primaryOnly
	w.isPrimary = isPrimary
}

// SetRunningJobKeys sets the shared map for tracking running job keys.
func (w *Worker) SetRunningJobKeys(runningJobKeys *sync.Map) {
	w.runningJobKeys = runningJobKeys
}

// SetReservationMutex sets the shared mutex for serializing job reservation.
func (w *Worker) SetReservationMutex(mutex *sync.Mutex) {
	w.reservationMutex = mutex
}

// Start begins processing jobs from the queue.
func (w *Worker) Start() {
	w.wg.Go(func() {
		slog.Info("Worker started", "worker_id", w.id)

		ticker := time.NewTicker(w.pollInterval)
		defer ticker.Stop()

		for {
			select {
			case <-w.ctx.Done():
				slog.Info("Worker stopped", "worker_id", w.id)
				return
			case <-ticker.C:
				if err := w.processNextJob(); err != nil {
					if err != sql.ErrNoRows {
						slog.Error("Worker error processing job", "worker_id", w.id, "error", err)
					}
				}
			}
		}
	})
}

// Stop gracefully stops the worker.
func (w *Worker) Stop() {
	w.cancel()
	w.wg.Wait()
}

// processNextJob attempts to reserve and process the next available job.
func (w *Worker) processNextJob() error {
	// Check if worker is shutting down before attempting to access database
	select {
	case <-w.ctx.Done():
		return nil
	default:
	}

	// If primary-only mode is enabled, skip processing if not primary.
	// In the future, Replicas will be allowed to process certain job types or
	// jobs that have been assigned to them by the Primary, while still relying
	// on the Primary for job dispatching, coordination, and storage.
	if w.primaryOnly && w.isPrimary != nil && !w.isPrimary() {
		return nil
	}

	// Acquire the reservation mutex to serialize job reservation across workers.
	// This prevents SQLite database lock errors when multiple workers try to
	// reserve jobs concurrently. The mutex is held only during reservation,
	// not during job execution, so workers can still process jobs in parallel.
	if w.reservationMutex != nil {
		w.reservationMutex.Lock()
		defer w.reservationMutex.Unlock()
	}

	db, err := w.systemDB.DB()

	if err != nil {
		// If database access fails due to shutdown, return nil to avoid logging errors
		if errors.Is(err, database.ErrorConnectionManagerShutdown) {
			return nil
		}
		return fmt.Errorf("failed to get system database: %w", err)
	}

	// Start a transaction to reserve the job
	tx, err := db.Begin()

	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer func() {
		if err := tx.Rollback(); err != nil && err != sql.ErrTxDone {
			slog.Error("Failed to rollback transaction", "error", err)
		}
	}()

	// Find the next available job
	now := time.Now().UTC()
	var queuedJob QueuedJob
	var dataJSON sql.NullString

	err = tx.QueryRow(`
		SELECT id, queue_name, name, key, status, attempts, max_attempts, data, created_at, updated_at, available_at
		FROM queued_jobs
		WHERE status = ? AND available_at <= ?
		ORDER BY available_at ASC
		LIMIT 1
	`, JobStatusPending, now.Format(time.RFC3339)).Scan(
		&queuedJob.ID,
		&queuedJob.QueueName,
		&queuedJob.Name,
		&queuedJob.Key,
		&queuedJob.Status,
		&queuedJob.Attempts,
		&queuedJob.MaxAttempts,
		&dataJSON,
		&queuedJob.CreatedAt,
		&queuedJob.UpdatedAt,
		&queuedJob.AvailableAt,
	)

	if err != nil {
		return err // Could be sql.ErrNoRows if no jobs available
	}

	// Deserialize job data
	var jobData map[string]any

	if dataJSON.Valid && dataJSON.String != "" {
		if err := json.Unmarshal([]byte(dataJSON.String), &jobData); err != nil {
			return fmt.Errorf("failed to unmarshal job data: %w", err)
		}
	}

	// Reserve the job
	reservedAt := now

	_, err = tx.Exec(`
		UPDATE queued_jobs
		SET status = ?, reserved_at = ?, reserved_by = ?, updated_at = ?
		WHERE id = ?
	`, JobStatusReserved, reservedAt.Format(time.RFC3339), w.id, now.Format(time.RFC3339), queuedJob.ID)

	if err != nil {
		return fmt.Errorf("failed to reserve job: %w", err)
	}

	// Commit the reservation
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit job reservation: %w", err)
	}

	// Process the job
	slog.Info("Processing job", "worker_id", w.id, "job_id", queuedJob.ID, "job_name", queuedJob.Name, "key", queuedJob.Key)

	// Get the job handler from the registry
	job, err := w.registry.Get(queuedJob.Name, jobData)

	if err != nil {
		// Job type not registered, mark as failed
		queuedJob.Attempts++
		w.markJobFailed(queuedJob.ID, queuedJob.Attempts, fmt.Sprintf("job type not registered: %v", err))

		if w.afterJob != nil {
			w.afterJob(queuedJob.ID, JobStatusFailed, err)
		}

		return fmt.Errorf("job type not registered: %w", err)
	}

	// Check if the job should be throttled
	shouldThrottle, throttleDelay := job.Throttle()

	if shouldThrottle {
		// Reschedule the job without incrementing attempts
		nextAvailableAt := time.Now().UTC().Add(throttleDelay)
		w.markJobForThrottle(queuedJob.ID, nextAvailableAt)
		slog.Info("Job throttled", "worker_id", w.id, "job_id", queuedJob.ID, "delay", throttleDelay)

		return nil
	}

	// Check for overlapping jobs (only if job has a key and withoutOverlap is enabled)
	if job.WithoutOverlap() && queuedJob.Key != "" && w.runningJobKeys != nil {
		// Try to mark this key as running
		_, alreadyRunning := w.runningJobKeys.LoadOrStore(queuedJob.Key, true)

		if alreadyRunning {
			// Another job with this key is already running, reschedule
			nextAvailableAt := time.Now().UTC().Add(job.OverlapRetryDelay())
			w.markJobForThrottle(queuedJob.ID, nextAvailableAt)
			slog.Info("Job overlapping", "worker_id", w.id, "job_id", queuedJob.ID, "key", queuedJob.Key, "delay", job.OverlapRetryDelay())

			return nil
		}

		// Ensure we remove the key when done (whether success or failure)
		defer w.runningJobKeys.Delete(queuedJob.Key)
	}

	// Execute the job
	// Create a context with timeout if specified
	ctx := context.Background()
	timeout := job.Timeout()

	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	err = job.Handle(ctx)

	if err != nil {
		slog.Error("Job failed", "worker_id", w.id, "job_id", queuedJob.ID, "error", err)

		// Increment attempts
		queuedJob.Attempts++

		// Check if we should retry
		if queuedJob.MaxAttempts == -1 || queuedJob.Attempts < queuedJob.MaxAttempts {
			// Retry the job
			retryAfter := job.RetryAfter()
			nextAvailableAt := time.Now().UTC().Add(retryAfter)

			w.markJobForRetry(queuedJob.ID, queuedJob.Attempts, nextAvailableAt, err.Error())
			slog.Info("Job scheduled for retry", "worker_id", w.id, "job_id", queuedJob.ID, "attempt", queuedJob.Attempts, "retry_after", retryAfter)
		} else {
			// Max attempts reached, mark as failed
			w.markJobFailed(queuedJob.ID, queuedJob.Attempts, err.Error())
			slog.Info("Job failed permanently", "worker_id", w.id, "job_id", queuedJob.ID, "attempts", queuedJob.Attempts)

			// Update batch status if this job is part of a batch
			if w.batchManager != nil {
				if batchErr := w.batchManager.UpdateJobStatus(w.ctx, queuedJob.ID); batchErr != nil {
					slog.Error("Failed to update batch status", "job_id", queuedJob.ID, "error", batchErr)
				}
			}

			if w.afterJob != nil {
				w.afterJob(queuedJob.ID, JobStatusFailed, err)
			}
		}

		return err
	}

	// Job succeeded, mark as completed
	w.markJobCompleted(queuedJob.ID)
	slog.Info("Job completed", "worker_id", w.id, "job_id", queuedJob.ID)

	// Update batch status if this job is part of a batch
	if w.batchManager != nil {
		if err := w.batchManager.UpdateJobStatus(w.ctx, queuedJob.ID); err != nil {
			slog.Error("Failed to update batch status", "job_id", queuedJob.ID, "error", err)
		}
	}

	if w.afterJob != nil {
		w.afterJob(queuedJob.ID, JobStatusCompleted, nil)
	}

	return nil
}

// markJobCompleted marks a job as successfully completed.
func (w *Worker) markJobCompleted(jobID int64) {
	// Check if worker is stopped
	if w.ctx.Err() != nil {
		return
	}

	db, err := w.systemDB.DB()

	if err != nil {
		slog.Error("Failed to get database for job completion", "job_id", jobID, "error", err)
		return
	}

	now := time.Now().UTC()

	_, err = db.Exec(`
		UPDATE queued_jobs
		SET status = ?, completed_at = ?, updated_at = ?
		WHERE id = ?
	`, JobStatusCompleted, now.Format(time.RFC3339), now.Format(time.RFC3339), jobID)

	if err != nil {
		slog.Error("Failed to mark job as completed", "job_id", jobID, "error", err)
		return
	}
}

// markJobFailed marks a job as permanently failed.
func (w *Worker) markJobFailed(jobID int64, attempts int, errorMsg string) {
	// Check if worker is stopped
	if w.ctx.Err() != nil {
		return
	}

	db, err := w.systemDB.DB()

	if err != nil {
		slog.Error("Failed to get database for job failure", "job_id", jobID, "error", err)

		return
	}

	now := time.Now().UTC()

	_, err = db.Exec(`
		UPDATE queued_jobs
		SET status = ?, attempts = ?, error_log = ?, updated_at = ?
		WHERE id = ?
	`, JobStatusFailed, attempts, errorMsg, now.Format(time.RFC3339), jobID)

	if err != nil {
		slog.Error("Failed to mark job as failed", "job_id", jobID, "error", err)
		return
	}
}

// markJobForRetry schedules a job for retry.
func (w *Worker) markJobForRetry(jobID int64, attempts int, availableAt time.Time, errorMsg string) {
	// Check if worker is stopped
	if w.ctx.Err() != nil {
		return
	}

	db, err := w.systemDB.DB()

	if err != nil {
		slog.Error("Failed to get database for job retry", "job_id", jobID, "error", err)

		return
	}

	now := time.Now().UTC()

	_, err = db.Exec(`
		UPDATE queued_jobs
		SET status = ?, attempts = ?, available_at = ?, error_log = ?, updated_at = ?, reserved_at = NULL, reserved_by = NULL
		WHERE id = ?
	`, JobStatusPending, attempts, availableAt.Format(time.RFC3339), errorMsg, now.Format(time.RFC3339), jobID)

	if err != nil {
		slog.Error("Failed to mark job for retry", "job_id", jobID, "error", err)
		return
	}
}

// markJobForThrottle reschedules a job that has been throttled.
// Unlike retry, throttling does NOT increment the attempts counter.
func (w *Worker) markJobForThrottle(jobID int64, availableAt time.Time) {
	// Check if worker is stopped
	if w.ctx.Err() != nil {
		return
	}

	db, err := w.systemDB.DB()

	if err != nil {
		slog.Error("Failed to get database for job throttle", "job_id", jobID, "error", err)
		return
	}

	now := time.Now().UTC()

	_, err = db.Exec(`
		UPDATE queued_jobs
		SET status = ?, available_at = ?, updated_at = ?, reserved_at = NULL, reserved_by = NULL
		WHERE id = ?
	`, JobStatusPending, availableAt.Format(time.RFC3339), now.Format(time.RFC3339), jobID)

	if err != nil {
		slog.Error("Failed to mark job for throttle", "job_id", jobID, "error", err)
		return
	}
}
