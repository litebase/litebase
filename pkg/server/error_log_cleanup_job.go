package server

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"

	"github.com/litebase/litebase/pkg/file"
)

// CleanupErrorLogJob deletes expired error log files
// for a specific database branch based on the retention period.
//
// Job data parameters:
//   - database_id: The ID of the database
//   - branch_id: The ID of the database branch
//   - cutoff_timestamp: Unix timestamp - files older than this will be deleted
//   - retention_days: Number of days to retain error logs
//   - branch_ref_id: The reference ID of the database branch in the system database
func (app *App) CleanupErrorLogJob(ctx context.Context, data map[string]any) error {
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

	slog.Info("Starting error log cleanup",
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

	deletedFiles := 0

	// Get error log directory path
	logBasePath := fmt.Sprintf("%slogs", file.GetDatabaseFileBaseDir(databaseID, branchID))

	// Read all error log files
	entries, err := tieredFS.ReadDir(logBasePath)

	if err != nil {
		if !os.IsNotExist(err) {
			slog.Error("Failed to read error log directory",
				"database_id", databaseID,
				"branch_id", branchID,
				"directory", logBasePath,
				"error", err)
			return fmt.Errorf("failed to read error log directory: %w", err)
		} else {
			slog.Debug("Error log directory does not exist, skipping",
				"database_id", databaseID,
				"branch_id", branchID)
			return nil
		}
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		// Parse timestamp from filename (format: error-{timestamp}.log)
		filename := entry.Name()

		if len(filename) < 11 || filename[:6] != "error-" || filename[len(filename)-4:] != ".log" {
			// Not an error log file, skip
			continue
		}

		// Extract timestamp between "error-" and ".log"
		timestampStr := filename[6 : len(filename)-4]
		timestamp, err := strconv.ParseInt(timestampStr, 10, 64)

		if err != nil {
			slog.Warn("Failed to parse error log file timestamp",
				"filename", filename,
				"error", err)
			continue
		}

		// Delete if older than cutoff
		if timestamp < cutoffTimestamp {
			filePath := fmt.Sprintf("%s/%s", logBasePath, filename)

			if err := tieredFS.Remove(filePath); err != nil {
				slog.Error("Failed to delete expired error log file",
					"database_id", databaseID,
					"branch_id", branchID,
					"file", filePath,
					"timestamp", timestamp,
					"error", err)
			} else {
				slog.Debug("Deleted expired error log file",
					"database_id", databaseID,
					"branch_id", branchID,
					"filename", filename,
					"timestamp", timestamp)
				deletedFiles++
			}
		}
	}

	slog.Info("Completed error log cleanup",
		"database_id", databaseID,
		"branch_id", branchID,
		"deleted_files", deletedFiles)

	return nil
}
