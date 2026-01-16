package server

import (
	"context"
	"log/slog"
	"time"
)

// PruneQueuedJobs removes completed and failed jobs from the queued_jobs table
// that are older than 30 days. This prevents the table from growing indefinitely.
func (app *App) PruneQueuedJobs(ctx context.Context) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	db, err := app.DatabaseManager.SystemDatabase().DB()

	if err != nil {
		slog.Error("Failed to get system database for pruning queued jobs", "error", err)
		return err
	}

	// Calculate cutoff timestamp (30 days ago)
	cutoffTime := time.Now().Add(-30 * 24 * time.Hour).Unix()

	slog.Info("Starting to prune old queued jobs", "cutoff_time", time.Unix(cutoffTime, 0))

	// Delete completed jobs older than 30 days
	completedResult, err := db.ExecContext(ctx, `
		DELETE FROM queued_jobs
		WHERE status = 'completed'
		AND updated_at < ?
	`, cutoffTime)

	if err != nil {
		slog.Error("Failed to delete old completed jobs", "error", err)
		return err
	}

	completedDeleted, _ := completedResult.RowsAffected()

	// Delete failed jobs older than 30 days
	failedResult, err := db.ExecContext(ctx, `
		DELETE FROM queued_jobs
		WHERE status = 'failed'
		AND updated_at < ?
	`, cutoffTime)

	if err != nil {
		slog.Error("Failed to delete old failed jobs", "error", err)
		return err
	}

	failedDeleted, _ := failedResult.RowsAffected()

	totalDeleted := completedDeleted + failedDeleted

	slog.Info("Completed pruning queued jobs",
		"completed_deleted", completedDeleted,
		"failed_deleted", failedDeleted,
		"total_deleted", totalDeleted,
		"cutoff_time", time.Unix(cutoffTime, 0))

	return nil
}
