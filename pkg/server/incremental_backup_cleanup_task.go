package server

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/litebase/litebase/pkg/queue"
)

// EnqueueIncrementalBackupCleanupJobs queries the system database for branches with
// expired incremental backups (snapshots and rollback logs) and dispatches cleanup jobs.
func (app *App) EnqueueIncrementalBackupCleanupJobs(ctx context.Context) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	db, err := app.DatabaseManager.SystemDatabase().DB()

	if err != nil {
		slog.Error("Failed to get system database for incremental backup cleanup", "error", err)
		return err
	}

	slog.Info("Starting to enqueue incremental backup cleanup jobs")

	pageSize := 1000
	offset := 0

	for {
		// Query branches that have incremental backups enabled
		rows, err := db.Query(`
			SELECT 
				db.database_id,
				db.database_branch_id,
				db.id as branch_ref_id,
				dbs.incremental_backups_retention_days
			FROM database_branches db
			INNER JOIN database_branch_settings dbs 
				ON dbs.database_branch_reference_id = db.id
			WHERE dbs.incremental_backups_enabled = 1
			LIMIT ? OFFSET ?
		`, pageSize, offset)

		if err != nil {
			slog.Error("Failed to query branches for incremental backup cleanup", "error", err)
			return err
		}

		count := 0

		for rows.Next() {
			count++

			var databaseID, branchID string
			var branchRefID int64
			var retentionDays int

			if err := rows.Scan(&databaseID, &branchID, &branchRefID, &retentionDays); err != nil {
				slog.Error("Failed to scan incremental backup cleanup row", "error", err)
				continue
			}

			// Calculate the expiration cutoff timestamp (in nanoseconds)
			// Files older than this should be deleted
			cutoffTime := time.Now().UTC().Add(-time.Duration(retentionDays) * 24 * time.Hour)
			cutoffTimestamp := cutoffTime.UnixNano()

			// Dispatch a cleanup job for this branch
			_, err := app.QueueDispatcher.DispatchJob(
				"CleanupIncrementalBackupJob",
				map[string]any{
					"database_id":      databaseID,
					"branch_id":        branchID,
					"cutoff_timestamp": cutoffTimestamp,
					"retention_days":   retentionDays,
					"branch_ref_id":    branchRefID,
				},
				queue.WithKey(fmt.Sprintf("cleanup-incremental:%s:%s", databaseID, branchID)),
				queue.Unique(),
			)

			if err != nil {
				slog.Error("Failed to dispatch incremental backup cleanup job",
					"database_id", databaseID,
					"branch_id", branchID,
					"error", err)
				// Continue processing other branches
			} else {
				slog.Debug("Dispatched incremental backup cleanup job",
					"database_id", databaseID,
					"branch_id", branchID,
					"retention_days", retentionDays)
			}
		}

		if err := rows.Close(); err != nil {
			slog.Error("Failed to close rows", "error", err)
			return err
		}

		// If we didn't get a full page, we're done
		if count < pageSize {
			break
		}

		offset += pageSize
	}

	slog.Info("Finished enqueueing incremental backup cleanup jobs")

	return nil
}
