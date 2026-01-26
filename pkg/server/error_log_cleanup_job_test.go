package server_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/file"
	"github.com/litebase/litebase/pkg/server"
)

func TestCleanupErrorLogJob(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		sysDB, err := app.DatabaseManager.SystemDatabase().DB()

		if err != nil {
			t.Fatalf("failed to get system database: %v", err)
		}

		// Helper to create mock error log files
		createMockErrorLogFile := func(db test.TestDatabase, daysAgo int) {
			branch, err := app.DatabaseManager.GetBranch(db.DatabaseID, db.DatabaseBranchID)

			if err != nil {
				t.Fatalf("failed to get database branch: %v", err)
			}

			resources := app.DatabaseManager.Resources(branch)
			tieredFS := resources.FileSystem().FileSystem()

			// Create error log directory
			errorLogPath := fmt.Sprintf("%slogs", file.GetDatabaseFileBaseDir(db.DatabaseID, db.DatabaseBranchID))

			if err := tieredFS.MkdirAll(errorLogPath, 0750); err != nil {
				t.Fatalf("failed to create error log directory: %v", err)
			}

			// Create a timestamp-based filename (format: error-{timestamp}.log)
			timestamp := time.Now().Add(-time.Duration(daysAgo) * 24 * time.Hour).Unix()
			filename := fmt.Sprintf("%s/error-%d.log", errorLogPath, timestamp)

			// Create the error log file
			logFile, err := tieredFS.Create(filename)

			if err != nil {
				t.Fatalf("failed to create error log file: %v", err)
			}

			if _, err := logFile.Write([]byte("mock error log data")); err != nil {
				t.Fatalf("failed to write error log data: %v", err)
			}

			if err := logFile.Close(); err != nil {
				t.Fatalf("failed to close error log file: %v", err)
			}
		}

		// Helper to count error log files
		countErrorLogFiles := func(db test.TestDatabase) int {
			branch, err := app.DatabaseManager.GetBranch(db.DatabaseID, db.DatabaseBranchID)

			if err != nil {
				return 0
			}

			resources := app.DatabaseManager.Resources(branch)
			tieredFS := resources.FileSystem().FileSystem()

			errorLogPath := fmt.Sprintf("%slogs", file.GetDatabaseFileBaseDir(db.DatabaseID, db.DatabaseBranchID))
			entries, err := tieredFS.ReadDir(errorLogPath)

			if err != nil {
				return 0
			}

			count := 0

			for _, entry := range entries {
				if !entry.IsDir() && len(entry.Name()) > 4 && entry.Name()[len(entry.Name())-4:] == ".log" {
					count++
				}
			}

			return count
		}

		t.Run("SuccessfulCleanup", func(t *testing.T) {
			db1 := test.MockDatabase(app)

			// Create old error logs (35 days ago - should be deleted with 30-day retention)
			createMockErrorLogFile(db1, 35)
			createMockErrorLogFile(db1, 40)

			// Create recent error logs (10 days ago - should be kept)
			createMockErrorLogFile(db1, 10)
			createMockErrorLogFile(db1, 5)

			// Get branch ref ID
			var branchRefID int64

			err := sysDB.QueryRow(`SELECT id FROM database_branches WHERE database_id = ? AND database_branch_id = ?`,
				db1.DatabaseID, db1.DatabaseBranchID).Scan(&branchRefID)

			if err != nil {
				t.Fatalf("failed to get branch ref id: %v", err)
			}

			// Verify files exist before cleanup
			if count := countErrorLogFiles(db1); count != 4 {
				t.Fatalf("Expected 4 error log files before cleanup, got %d", count)
			}

			// Run cleanup job with 30-day retention
			ctx := context.Background()

			cutoffTime := time.Now().UTC().Add(-30 * 24 * time.Hour)

			data := map[string]any{
				"database_id":      db1.DatabaseID,
				"branch_id":        db1.DatabaseBranchID,
				"cutoff_timestamp": float64(cutoffTime.Unix()),
				"retention_days":   float64(30),
				"branch_ref_id":    float64(branchRefID),
			}

			err = app.CleanupErrorLogJob(ctx, data)

			if err != nil {
				t.Fatalf("CleanupErrorLogJob failed: %v", err)
			}

			// Verify only old files were deleted (2 should remain)
			if count := countErrorLogFiles(db1); count != 2 {
				t.Errorf("Expected 2 error log files after cleanup, got %d", count)
			}
		})

		t.Run("MissingDirectory", func(t *testing.T) {
			db1 := test.MockDatabase(app)

			// Get branch ref ID
			var branchRefID int64
			err := sysDB.QueryRow(`SELECT id FROM database_branches WHERE database_id = ? AND database_branch_id = ?`,
				db1.DatabaseID, db1.DatabaseBranchID).Scan(&branchRefID)

			if err != nil {
				t.Fatalf("failed to get branch ref id: %v", err)
			}

			// Run cleanup (directory doesn't exist, should not error)
			ctx := context.Background()
			cutoffTime := time.Now().UTC().Add(-30 * 24 * time.Hour)
			data := map[string]any{
				"database_id":      db1.DatabaseID,
				"branch_id":        db1.DatabaseBranchID,
				"cutoff_timestamp": float64(cutoffTime.Unix()),
				"retention_days":   float64(30),
				"branch_ref_id":    float64(branchRefID),
			}

			err = app.CleanupErrorLogJob(ctx, data)

			if err != nil {
				t.Fatalf("CleanupErrorLogJob should not fail for missing directory: %v", err)
			}
		})

		t.Run("MissingDatabaseID", func(t *testing.T) {
			ctx := context.Background()
			data := map[string]any{
				"branch_id":        "test-branch",
				"cutoff_timestamp": float64(123456),
				"retention_days":   float64(30),
				"branch_ref_id":    float64(1),
			}

			err = app.CleanupErrorLogJob(ctx, data)

			if err == nil {
				t.Error("Expected error for missing database_id")
			}
		})

		t.Run("InvalidCutoffTimestamp", func(t *testing.T) {
			ctx := context.Background()
			data := map[string]any{
				"database_id":      "test-db",
				"branch_id":        "test-branch",
				"cutoff_timestamp": "not-a-number",
				"retention_days":   float64(30),
				"branch_ref_id":    float64(1),
			}

			err := app.CleanupErrorLogJob(ctx, data)

			if err == nil {
				t.Error("Expected error for invalid cutoff_timestamp type")
			}
		})

		t.Run("NoFilesToCleanup", func(t *testing.T) {
			db1 := test.MockDatabase(app)

			// Create only recent error logs (within retention period)
			createMockErrorLogFile(db1, 5)
			createMockErrorLogFile(db1, 10)

			// Get branch ref ID
			var branchRefID int64
			err := sysDB.QueryRow(`SELECT id FROM database_branches WHERE database_id = ? AND database_branch_id = ?`,
				db1.DatabaseID, db1.DatabaseBranchID).Scan(&branchRefID)

			if err != nil {
				t.Fatalf("failed to get branch ref id: %v", err)
			}

			beforeCount := countErrorLogFiles(db1)

			// Run cleanup with 30-day retention
			ctx := context.Background()
			cutoffTime := time.Now().UTC().Add(-30 * 24 * time.Hour)

			data := map[string]any{
				"database_id":      db1.DatabaseID,
				"branch_id":        db1.DatabaseBranchID,
				"cutoff_timestamp": float64(cutoffTime.Unix()),
				"retention_days":   float64(30),
				"branch_ref_id":    float64(branchRefID),
			}

			err = app.CleanupErrorLogJob(ctx, data)

			if err != nil {
				t.Fatalf("CleanupErrorLogJob failed: %v", err)
			}

			// Verify no files were deleted
			afterCount := countErrorLogFiles(db1)

			if afterCount != beforeCount {
				t.Errorf("Expected no files to be deleted, before=%d after=%d", beforeCount, afterCount)
			}
		})

		t.Run("InvalidFilenameFormat", func(t *testing.T) {
			db1 := test.MockDatabase(app)

			branch, err := app.DatabaseManager.GetBranch(db1.DatabaseID, db1.DatabaseBranchID)

			if err != nil {
				t.Fatalf("failed to get database branch: %v", err)
			}

			resources := app.DatabaseManager.Resources(branch)
			tieredFS := resources.FileSystem().FileSystem()

			// Create error log directory
			errorLogPath := fmt.Sprintf("%slogs", file.GetDatabaseFileBaseDir(db1.DatabaseID, db1.DatabaseBranchID))

			if err := tieredFS.MkdirAll(errorLogPath, 0750); err != nil {
				t.Fatalf("failed to create error log directory: %v", err)
			}

			// Create files with invalid formats (should be skipped)
			invalidFiles := []string{
				fmt.Sprintf("%s/invalid.log", errorLogPath),
				fmt.Sprintf("%s/query_log.txt", errorLogPath),
				fmt.Sprintf("%s/error-notanumber.log", errorLogPath),
			}

			for _, filename := range invalidFiles {
				logFile, err := tieredFS.Create(filename)

				if err != nil {
					t.Fatalf("failed to create invalid log file: %v", err)
				}

				if err := logFile.Close(); err != nil {
					t.Fatalf("failed to close invalid log file: %v", err)
				}
			}

			// Get branch ref ID
			var branchRefID int64
			err = sysDB.QueryRow(`SELECT id FROM database_branches WHERE database_id = ? AND database_branch_id = ?`,
				db1.DatabaseID, db1.DatabaseBranchID).Scan(&branchRefID)

			if err != nil {
				t.Fatalf("failed to get branch ref id: %v", err)
			}

			// Run cleanup
			ctx := context.Background()
			cutoffTime := time.Now().UTC().Add(-30 * 24 * time.Hour)

			data := map[string]any{
				"database_id":      db1.DatabaseID,
				"branch_id":        db1.DatabaseBranchID,
				"cutoff_timestamp": float64(cutoffTime.Unix()),
				"retention_days":   float64(30),
				"branch_ref_id":    float64(branchRefID),
			}

			err = app.CleanupErrorLogJob(ctx, data)

			if err != nil {
				t.Fatalf("CleanupErrorLogJob should not fail with invalid filenames: %v", err)
			}

			// Verify invalid files still exist (should be skipped)
			entries, err := tieredFS.ReadDir(errorLogPath)

			if err != nil {
				t.Fatalf("failed to read error log directory: %v", err)
			}

			if len(entries) != 3 {
				t.Errorf("Expected 3 invalid files to remain, got %d", len(entries))
			}
		})
	})
}
