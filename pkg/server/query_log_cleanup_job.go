package server

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"

	"github.com/litebase/litebase/pkg/file"
)

// CleanupQueryLogJob deletes expired query log directories (containing query logs and statement indexes)
// for a specific database branch based on the retention period.
//
// Job data parameters:
//   - database_id: The ID of the database
//   - branch_id: The ID of the database branch
//   - cutoff_timestamp: Unix timestamp - directories older than this will be deleted
//   - retention_days: Number of days to retain query logs
//   - branch_ref_id: The reference ID of the database branch in the system database
func (app *App) CleanupQueryLogJob(ctx context.Context, data map[string]any) error {
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

	slog.Info("Starting query log cleanup",
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

	deletedDirectories := 0

	// Get query log directory path
	queryLogPath := fmt.Sprintf("%slogs/query", file.GetDatabaseFileBaseDir(databaseID, branchID))

	// Read all timestamp directories
	entries, err := tieredFS.ReadDir(queryLogPath)

	if err != nil {
		if !os.IsNotExist(err) {
			slog.Error("Failed to read query log directory",
				"database_id", databaseID,
				"branch_id", branchID,
				"directory", queryLogPath,
				"error", err)
			return fmt.Errorf("failed to read query log directory: %w", err)
		} else {
			slog.Debug("Query log directory does not exist, skipping",
				"database_id", databaseID,
				"branch_id", branchID)
			return nil
		}
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		// Parse timestamp from directory name
		timestamp, err := strconv.ParseInt(entry.Name(), 10, 64)

		if err != nil {
			slog.Warn("Failed to parse query log directory timestamp",
				"directory", entry.Name(),
				"error", err)
			continue
		}

		// Delete if older than cutoff
		if timestamp < cutoffTimestamp {
			dirPath := fmt.Sprintf("%s/%s", queryLogPath, entry.Name())

			if err := tieredFS.RemoveAll(dirPath); err != nil {
				slog.Error("Failed to delete expired query log directory",
					"database_id", databaseID,
					"branch_id", branchID,
					"directory", dirPath,
					"timestamp", timestamp,
					"error", err)
			} else {
				slog.Debug("Deleted expired query log directory",
					"database_id", databaseID,
					"branch_id", branchID,
					"directory", entry.Name(),
					"timestamp", timestamp)
				deletedDirectories++
			}
		}
	}

	slog.Info("Completed query log cleanup",
		"database_id", databaseID,
		"branch_id", branchID,
		"deleted_directories", deletedDirectories)

	return nil
}
