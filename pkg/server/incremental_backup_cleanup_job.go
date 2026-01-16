package server

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"time"

	"github.com/litebase/litebase/pkg/file"
)

// CleanupIncrementalBackupJob deletes expired incremental backup files (snapshots and rollback logs)
// for a specific database branch based on the retention period.
//
// Job data parameters:
//   - database_id: The ID of the database
//   - branch_id: The ID of the database branch
//   - cutoff_timestamp: Unix nanoseconds timestamp - files older than this will be deleted
//   - retention_days: Number of days to retain incremental backups
//   - branch_ref_id: The reference ID of the database branch in the system database
func (app *App) CleanupIncrementalBackupJob(ctx context.Context, data map[string]any) error {
	// Extract job parameters
	databaseID, ok := data["database_id"].(string)

	if !ok {
		return fmt.Errorf("missing or invalid database_id")
	}

	branchID, ok := data["branch_id"].(string)

	if !ok {
		return fmt.Errorf("missing or invalid branch_id")
	}

	cutoffTimestampFloat, ok := data["cutoff_timestamp"].(float64)

	if !ok {
		return fmt.Errorf("missing or invalid cutoff_timestamp")
	}

	cutoffTimestamp := int64(cutoffTimestampFloat)

	retentionDaysFloat, ok := data["retention_days"].(float64)

	if !ok {
		return fmt.Errorf("missing or invalid retention_days")
	}

	retentionDays := int(retentionDaysFloat)

	branchRefIDFloat, ok := data["branch_ref_id"].(float64)

	if !ok {
		return fmt.Errorf("missing or invalid branch_ref_id")
	}

	branchRefID := int64(branchRefIDFloat)

	slog.Info("Starting incremental backup cleanup",
		"database_id", databaseID,
		"branch_id", branchID,
		"retention_days", retentionDays,
		"cutoff_timestamp", cutoffTimestamp)

	// Get the database branch to access tiered filesystem
	branch, err := app.DatabaseManager.GetBranch(databaseID, branchID)

	if err != nil {
		return fmt.Errorf("failed to get database branch: %w", err)
	}

	resources := app.DatabaseManager.Resources(branch)
	tieredFS := resources.FileSystem().FileSystem()

	deletedSnapshots := 0
	deletedRollbackLogs := 0

	// Clean up snapshots
	snapshotDir := file.GetDatabaseSnapshotDirectory(databaseID, branchID)
	snapshotEntries, err := tieredFS.ReadDir(snapshotDir)

	if err != nil {
		if !os.IsNotExist(err) {
			slog.Error("Failed to read snapshot directory",
				"database_id", databaseID,
				"branch_id", branchID,
				"directory", snapshotDir,
				"error", err)
		} else {
			slog.Debug("Snapshot directory does not exist, skipping",
				"database_id", databaseID,
				"branch_id", branchID)
		}
	} else {
		for _, entry := range snapshotEntries {
			if entry.IsDir() {
				continue
			}

			// Parse timestamp from filename
			timestamp, err := strconv.ParseInt(entry.Name(), 10, 64)

			if err != nil {
				slog.Warn("Failed to parse snapshot timestamp",
					"file", entry.Name(),
					"error", err)
				continue
			}

			// Delete if older than cutoff
			if timestamp < cutoffTimestamp {
				snapshotPath := fmt.Sprintf("%s/%s", snapshotDir, entry.Name())

				if err := tieredFS.Remove(snapshotPath); err != nil {
					slog.Error("Failed to delete expired snapshot",
						"database_id", databaseID,
						"branch_id", branchID,
						"file", snapshotPath,
						"error", err)
				} else {
					slog.Debug("Deleted expired snapshot",
						"database_id", databaseID,
						"branch_id", branchID,
						"file", entry.Name(),
						"timestamp", timestamp)

					deletedSnapshots++
				}
			}
		}
	}

	// Clean up rollback logs
	rollbackDir := file.GetDatabaseRollbackDirectory(databaseID, branchID)
	rollbackEntries, err := tieredFS.ReadDir(rollbackDir)

	if err != nil {
		if !os.IsNotExist(err) {
			slog.Error("Failed to read rollback directory",
				"database_id", databaseID,
				"branch_id", branchID,
				"directory", rollbackDir,
				"error", err)
		} else {
			slog.Debug("Rollback directory does not exist, skipping",
				"database_id", databaseID,
				"branch_id", branchID)
		}
	} else {
		for _, entry := range rollbackEntries {
			if entry.IsDir() {
				continue
			}

			// Parse timestamp from filename
			timestamp, err := strconv.ParseInt(entry.Name(), 10, 64)

			if err != nil {
				slog.Warn("Failed to parse rollback log timestamp",
					"file", entry.Name(),
					"error", err)
				continue
			}

			// Delete if older than cutoff
			if timestamp < cutoffTimestamp {
				rollbackPath := fmt.Sprintf("%s/%s", rollbackDir, entry.Name())

				if err := tieredFS.Remove(rollbackPath); err != nil {
					slog.Error("Failed to delete expired rollback log",
						"database_id", databaseID,
						"branch_id", branchID,
						"file", rollbackPath,
						"error", err)
				} else {
					slog.Debug("Deleted expired rollback log",
						"database_id", databaseID,
						"branch_id", branchID,
						"file", entry.Name(),
						"timestamp", timestamp)

					deletedRollbackLogs++
				}
			}
		}
	}

	// Update the incremental_backups_cleaned_at timestamp
	sysDB, err := app.DatabaseManager.SystemDatabase().DB()

	if err != nil {
		slog.Error("Failed to get system DB for cleanup timestamp update",
			"database_id", databaseID,
			"branch_id", branchID,
			"error", err)

		return fmt.Errorf("failed to get system database: %w", err)
	}

	now := time.Now().Unix()

	_, err = sysDB.Exec(`
		UPDATE database_branch_settings
		SET incremental_backups_cleaned_at = ?
		WHERE database_branch_reference_id = ?
	`, now, branchRefID)

	if err != nil {
		slog.Error("Failed to update incremental_backups_cleaned_at",
			"database_id", databaseID,
			"branch_id", branchID,
			"error", err)

		return fmt.Errorf("failed to update cleanup timestamp: %w", err)
	}

	slog.Info("Completed incremental backup cleanup",
		"database_id", databaseID,
		"branch_id", branchID,
		"deleted_snapshots", deletedSnapshots,
		"deleted_rollback_logs", deletedRollbackLogs)

	return nil
}
