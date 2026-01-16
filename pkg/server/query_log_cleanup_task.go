package server

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/litebase/litebase/pkg/queue"
)

// EnqueueQueryLogCleanupJobs queries the system database for branches with
// query logs enabled and dispatches cleanup jobs for expired query logs.
func (app *App) EnqueueQueryLogCleanupJobs(ctx context.Context) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	db, err := app.DatabaseManager.SystemDatabase().DB()

	if err != nil {
		slog.Error("Failed to get system database for query log cleanup", "error", err)
		return err
	}

	slog.Info("Starting to enqueue query log cleanup jobs")

	pageSize := 1000
	offset := 0

	for {
		// Query all branches (query logs are enabled by default)
		// We'll use a default retention of 30 days if not specified
		rows, err := db.Query(`
			SELECT 
				db.database_id,
				db.database_branch_id,
				db.id as branch_ref_id
			FROM database_branches db
			LIMIT ? OFFSET ?
		`, pageSize, offset)

		if err != nil {
			slog.Error("Failed to query branches for query log cleanup", "error", err)
			return err
		}

		count := 0

		for rows.Next() {
			count++

			var databaseID, branchID string
			var branchRefID int64

			if err := rows.Scan(&databaseID, &branchID, &branchRefID); err != nil {
				slog.Error("Failed to scan query log cleanup row", "error", err)
				continue
			}

			// Default retention: 30 days for query logs
			retentionDays := 30
			cutoffTime := time.Now().UTC().Add(-time.Duration(retentionDays) * 24 * time.Hour)
			cutoffTimestamp := cutoffTime.Unix()

			// Dispatch a cleanup job for this branch
			_, err := app.QueueDispatcher.DispatchJob(
				"CleanupQueryLogJob",
				map[string]any{
					"database_id":      databaseID,
					"branch_id":        branchID,
					"cutoff_timestamp": cutoffTimestamp,
					"retention_days":   retentionDays,
					"branch_ref_id":    branchRefID,
				},
				queue.WithKey(fmt.Sprintf("cleanup-query-log:%s:%s", databaseID, branchID)),
				queue.Unique(),
			)

			if err != nil {
				slog.Error("Failed to dispatch query log cleanup job",
					"database_id", databaseID,
					"branch_id", branchID,
					"error", err)
				// Continue processing other branches
			} else {
				slog.Debug("Dispatched query log cleanup job",
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

	slog.Info("Finished enqueueing query log cleanup jobs")

	return nil
}
