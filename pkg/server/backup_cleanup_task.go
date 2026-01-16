package server

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/litebase/litebase/pkg/queue"
)

// EnqueueBackupCleanupJobs queries the system database for expired backups
// and dispatches a unique CleanupBackupJob for each expired backup.
func (app *App) EnqueueBackupCleanupJobs(ctx context.Context) error {
	if app.DatabaseManager == nil || app.QueueDispatcher == nil {
		return nil
	}

	db, err := app.DatabaseManager.SystemDatabase().DB()

	if err != nil {
		slog.Error("Failed to get system database for backup cleanup", "error", err)
		return err
	}

	const pageSize = 1000
	lastID := int64(0)

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// Query for expired backups by joining with branch settings to get retention days.
		// A backup is expired if: created_at < now - (retention_days * 24 hours)
		rows, err := db.Query(`
			SELECT 
				b.id,
				b.database_id,
				b.database_branch_id,
				b.restore_point_timestamp,
				b.created_at,
				s.backups_retention_days
			FROM database_backups b
			JOIN database_branches br ON br.id = b.database_branch_reference_id
			JOIN database_branch_settings s ON s.database_branch_reference_id = br.id
			WHERE datetime(b.created_at) < datetime('now', '-' || s.backups_retention_days || ' days')
			  AND b.id > ?
			ORDER BY b.id ASC
			LIMIT ?
		`, lastID, pageSize)

		if err != nil {
			slog.Error("Failed to query expired backups (paged)", "error", err)
			return err
		}

		count := 0

		for rows.Next() {
			var (
				backupID             int64
				databaseID           string
				branchID             string
				restorePointTimestamp int64
				createdAt            string
				retentionDays        int
			)

			if err := rows.Scan(&backupID, &databaseID, &branchID, &restorePointTimestamp, &createdAt, &retentionDays); err != nil {
				slog.Error("Failed to scan expired backup row", "error", err)
				continue
			}

			count++
			lastID = backupID

			data := map[string]any{
				"backup_id":               backupID,
				"database_id":             databaseID,
				"branch_id":               branchID,
				"restore_point_timestamp": restorePointTimestamp,
			}

			key := fmt.Sprintf("cleanup-backup:%d", backupID)

			_, err := app.QueueDispatcher.DispatchJob("CleanupBackupJob", data, queue.WithKey(key), queue.Unique())

			if err != nil {
				slog.Error("Failed to dispatch CleanupBackupJob", "backup_id", backupID, "database_id", databaseID, "branch_id", branchID, "error", err)
				continue
			}

			slog.Info("Dispatched CleanupBackupJob", "backup_id", backupID, "database_id", databaseID, "branch_id", branchID, "created_at", createdAt, "retention_days", retentionDays)
		}

		err = rows.Close()

		if err != nil {
			slog.Error("Failed to close rows after processing expired backups", "error", err)
			return err
		}

		if count < pageSize {
			break
		}
	}

	return nil
}
