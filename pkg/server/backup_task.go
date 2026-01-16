package server

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"github.com/litebase/litebase/pkg/queue"
)

// EnqueueBackupJobs queries the system database for branches with backups enabled
// and dispatches a unique BackupJob for each branch.
func (app *App) EnqueueBackupJobs(ctx context.Context) error {
	if app.DatabaseManager == nil || app.QueueDispatcher == nil {
		return nil
	}

	db, err := app.DatabaseManager.SystemDatabase().DB()

	if err != nil {
		slog.Error("Failed to get system database for backup enqueue", "error", err)
		return err
	}

	const pageSize = 1000
	lastID := int64(0)
	nowUnix := time.Now().UTC().Unix()

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// Select due branches in pages to avoid loading massive result sets.
		rows, err := db.Query(`
			SELECT b.id, b.database_id, b.database_branch_id, s.backups_interval
			FROM database_branches b
			JOIN database_branch_settings s ON s.database_branch_reference_id = b.id
			WHERE s.backups_enabled = 1
			  AND (s.backup_next_at IS NULL OR s.backup_next_at <= ?)
			  AND b.id > ?
			ORDER BY b.id ASC
			LIMIT ?
		`, nowUnix, lastID, pageSize)

		if err != nil {
			slog.Error("Failed to query branches with backups enabled (paged)", "error", err)
			return err
		}

		count := 0

		for rows.Next() {
			var (
				branchRefID     int64
				databaseID      string
				branchID        string
				backupsInterval sql.NullString
			)

			if err := rows.Scan(&branchRefID, &databaseID, &branchID, &backupsInterval); err != nil {
				slog.Error("Failed to scan backup branch row", "error", err)
				continue
			}

			count++
			lastID = branchRefID

			data := map[string]any{
				"database_id": databaseID,
				"branch_id":   branchID,
			}

			key := fmt.Sprintf("backup:%s:%s", databaseID, branchID)

			_, err := app.QueueDispatcher.DispatchJob("BackupJob", data, queue.WithKey(key), queue.Unique())

			if err != nil {
				slog.Error("Failed to dispatch BackupJob", "database_id", databaseID, "branch_id", branchID, "error", err)
				continue
			}

			slog.Info("Dispatched BackupJob", "database_id", databaseID, "branch_id", branchID)
		}

		err = rows.Close()

		if err != nil {
			slog.Error("Failed to close rows after processing backup branches", "error", err)
			return err
		}

		if count < pageSize {
			break
		}
	}

	return nil
}
