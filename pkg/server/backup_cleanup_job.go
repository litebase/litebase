package server

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/litebase/litebase/pkg/file"
)

// CleanupBackupJob deletes an expired backup and its associated files from storage.
func (app *App) CleanupBackupJob(ctx context.Context, data map[string]any) error {
	backupID, ok := data["backup_id"].(float64)

	if !ok {
		return fmt.Errorf("backup_id is required and must be a number")
	}

	databaseID, ok := data["database_id"].(string)

	if !ok || databaseID == "" {
		return fmt.Errorf("database_id is required")
	}

	branchID, ok := data["branch_id"].(string)

	if !ok || branchID == "" {
		return fmt.Errorf("branch_id is required")
	}

	restorePointTimestamp, ok := data["restore_point_timestamp"].(float64)

	if !ok {
		return fmt.Errorf("restore_point_timestamp is required and must be a number")
	}

	slog.Info("Starting backup cleanup", "backup_id", int64(backupID), "database_id", databaseID, "branch_id", branchID, "restore_point_timestamp", int64(restorePointTimestamp))

	// Delete the backup files from object storage
	// Note: Don't include trailing slash for RemoveAll
	backupDir := fmt.Sprintf(
		"%s%d",
		file.GetDatabaseBackupsDirectory(databaseID, branchID),
		int64(restorePointTimestamp),
	)

	objectFS := app.Cluster.ObjectFS()

	// Remove the backup directory and all its contents
	err := objectFS.RemoveAll(backupDir)

	if err != nil {
		// Log error but continue - we still want to remove the database record
		slog.Error("Failed to remove backup directory", "backup_id", int64(backupID), "directory", backupDir, "error", err)
	} else {
		slog.Debug("Deleted backup directory", "backup_id", int64(backupID), "directory", backupDir)
	}

	// Delete the backup record from the database
	db, err := app.DatabaseManager.SystemDatabase().DB()

	if err != nil {
		slog.Error("Failed to get system DB for backup cleanup", "backup_id", int64(backupID), "error", err)
		return fmt.Errorf("failed to get system database: %w", err)
	}

	result, err := db.Exec(`DELETE FROM database_backups WHERE id = ?`, int64(backupID))

	if err != nil {
		slog.Error("Failed to delete backup record", "backup_id", int64(backupID), "error", err)
		return fmt.Errorf("failed to delete backup record: %w", err)
	}

	rowsAffected, err := result.RowsAffected()

	if err != nil {
		slog.Warn("Could not determine rows affected for backup deletion", "backup_id", int64(backupID), "error", err)
	} else if rowsAffected == 0 {
		slog.Warn("Backup record not found in database (may have been deleted already)", "backup_id", int64(backupID))
	}

	// Update the backups_cleaned_at timestamp for the branch
	_, err = db.Exec(`
		UPDATE database_branch_settings 
		SET backups_cleaned_at = ? 
		WHERE database_branch_reference_id = (
			SELECT id FROM database_branches 
			WHERE database_id = ? AND database_branch_id = ?
		)
	`, time.Now().UTC().Unix(), databaseID, branchID)

	if err != nil {
		slog.Warn("Failed to update backups_cleaned_at timestamp", "database_id", databaseID, "branch_id", branchID, "error", err)
		// Don't fail the job for this
	}

	slog.Info("Backup cleanup completed successfully", "backup_id", int64(backupID), "database_id", databaseID, "branch_id", branchID)

	return nil
}
