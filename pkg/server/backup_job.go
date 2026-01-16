package server

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"github.com/litebase/litebase/pkg/backups"
)

// BackupJob executes a backup job with the given data
func (app *App) BackupJob(ctx context.Context, data map[string]any) error {
	databaseID, ok := data["database_id"].(string)

	if !ok || databaseID == "" {
		return fmt.Errorf("database_id is required")
	}

	branchID, ok := data["branch_id"].(string)

	if !ok || branchID == "" {
		return fmt.Errorf("branch_id is required")
	}

	branch, err := app.DatabaseManager.GetBranch(databaseID, branchID)

	if err != nil {
		slog.Error("Failed to get database branch for backup job", "error", err, "database_id", databaseID, "branch_id", branchID)
		return err
	}

	backup, err := backups.Run(
		app.Cluster.Config,
		app.Cluster.ObjectFS(),
		branch.DatabaseID,
		branch.DatabaseBranchID,
		app.DatabaseManager.Resources(branch).SnapshotLogger(),
		app.DatabaseManager.Resources(branch).FileSystem(),
		app.DatabaseManager.Resources(branch).RollbackLogger(),
	)

	if err != nil {
		return err
	}

	// Store the database backup in the system database.
	err = app.DatabaseManager.SystemDatabase().StoreDatabaseBackup(
		branch.ID,
		branch.ID,
		branch.DatabaseID,
		branch.DatabaseBranchID,
		backup.RestorePoint.Timestamp,
		backup.RestorePoint.PageCount,
		backup.GetSize(),
	)

	if err != nil {
		slog.Error("Failed to store database backup", "error", err, "databaseId", branch.DatabaseID, "branchId", branch.DatabaseBranchID)
		return err
	}

	db, err := app.DatabaseManager.SystemDatabase().DB()

	if err != nil {
		slog.Error("Failed to get system DB to update backup_next_at", "error", err)
		return nil
	}

	var branchRefID int64
	err = db.QueryRow(`SELECT id FROM database_branches WHERE database_id = ? AND database_branch_id = ?`, databaseID, branchID).Scan(&branchRefID)

	if err != nil {
		if err != sql.ErrNoRows {
			slog.Error("Failed to find branch ref id to update backup_next_at", "error", err, "database_id", databaseID, "branch_id", branchID)
		}

		return nil
	}

	var backupsInterval sql.NullString
	var currentNextAt sql.NullInt64

	err = db.QueryRow(`SELECT backups_interval, backup_next_at FROM database_branch_settings WHERE database_branch_reference_id = ?`, branchRefID).Scan(&backupsInterval, &currentNextAt)

	if err != nil {
		slog.Error("Failed to read backup settings for branch", "error", err, "branch_ref_id", branchRefID)
		return nil
	}

	intervalStr := "24h"

	if backupsInterval.Valid && backupsInterval.String != "" {
		intervalStr = backupsInterval.String
	}

	dur, err := time.ParseDuration(intervalStr)

	if err != nil || dur < 24*time.Hour {
		dur = 24 * time.Hour
	}

	// Calculate next backup based on the interval.
	// Start from the scheduled time (currentNextAt) and add the interval.
	// If the backup ran late and the next scheduled time would be in the past,
	// keep adding intervals until we get a future time to avoid drift.
	var nextAt int64
	now := time.Now().UTC().Unix()

	if currentNextAt.Valid && currentNextAt.Int64 > 0 {
		nextAt = currentNextAt.Int64
		intervalSeconds := int64(dur.Seconds())

		// Add intervals until we get a time in the future
		for nextAt <= now {
			nextAt += intervalSeconds
		}
	} else {
		// No previous schedule, start from now + interval
		nextAt = time.Now().UTC().Add(dur).Unix()
	}

	if _, err := db.Exec(`UPDATE database_branch_settings SET backup_next_at = ?, updated_at = ? WHERE database_branch_reference_id = ?`, nextAt, time.Now().UTC().Unix(), branchRefID); err != nil {
		slog.Error("Failed to update backup_next_at after job success", "branch_ref_id", branchRefID, "error", err)
	} else {
		slog.Info("Updated backup_next_at after backup completion", "branch_ref_id", branchRefID, "next_at", nextAt)
	}

	return nil
}
