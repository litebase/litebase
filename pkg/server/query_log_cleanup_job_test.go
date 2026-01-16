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

func TestCleanupQueryLogJob(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		sysDB, err := app.DatabaseManager.SystemDatabase().DB()

		if err != nil {
			t.Fatalf("failed to get system database: %v", err)
		}

		// Helper to create mock query log directories
		createMockQueryLogDirs := func(db test.TestDatabase, daysAgo int) {
			branch, err := app.DatabaseManager.GetBranch(db.DatabaseID, db.DatabaseBranchID)

			if err != nil {
				t.Fatalf("failed to get database branch: %v", err)
			}

			resources := app.DatabaseManager.Resources(branch)
			tieredFS := resources.FileSystem().FileSystem()

			// Create a timestamp directory
			timestamp := time.Now().Add(-time.Duration(daysAgo) * 24 * time.Hour).Unix()
			queryLogPath := fmt.Sprintf("%slogs/query", file.GetDatabaseFileBaseDir(db.DatabaseID, db.DatabaseBranchID))
			dirPath := fmt.Sprintf("%s/%d", queryLogPath, timestamp)

			if err := tieredFS.MkdirAll(dirPath, 0750); err != nil {
				t.Fatalf("failed to create query log directory: %v", err)
			}

			// Create a mock query log file
			logFile, err := tieredFS.Create(fmt.Sprintf("%s/QUERY_LOG_test", dirPath))

			if err != nil {
				t.Fatalf("failed to create query log file: %v", err)
			}

			if _, err := logFile.Write([]byte("mock query log data")); err != nil {
				t.Fatalf("failed to write query log data: %v", err)
			}

			if err := logFile.Close(); err != nil {
				t.Fatalf("failed to close query log file: %v", err)
			}

			// Create a mock statement index file
			indexFile, err := tieredFS.Create(fmt.Sprintf("%s/QUERY_STATEMENT_INDEX_test", dirPath))

			if err != nil {
				t.Fatalf("failed to create statement index file: %v", err)
			}

			if _, err := indexFile.Write([]byte("mock statement index data")); err != nil {
				t.Fatalf("failed to write statement index data: %v", err)
			}

			if err := indexFile.Close(); err != nil {
				t.Fatalf("failed to close statement index file: %v", err)
			}
		}

		// Helper to count query log directories
		countQueryLogDirs := func(db test.TestDatabase) int {
			branch, err := app.DatabaseManager.GetBranch(db.DatabaseID, db.DatabaseBranchID)

			if err != nil {
				return 0
			}

			resources := app.DatabaseManager.Resources(branch)
			tieredFS := resources.FileSystem().FileSystem()

			queryLogPath := fmt.Sprintf("%slogs/query", file.GetDatabaseFileBaseDir(db.DatabaseID, db.DatabaseBranchID))
			entries, err := tieredFS.ReadDir(queryLogPath)

			if err != nil {
				return 0
			}

			count := 0

			for _, entry := range entries {
				if entry.IsDir() {
					count++
				}
			}

			return count
		}

		t.Run("SuccessfulCleanup", func(t *testing.T) {
			db1 := test.MockDatabase(app)

			// Create old query logs (35 days ago - should be deleted with 30-day retention)
			createMockQueryLogDirs(db1, 35)
			createMockQueryLogDirs(db1, 40)

			// Create recent query logs (10 days ago - should be kept)
			createMockQueryLogDirs(db1, 10)
			createMockQueryLogDirs(db1, 5)

			// Get branch ref ID
			var branchRefID int64

			err := sysDB.QueryRow(`SELECT id FROM database_branches WHERE database_id = ? AND database_branch_id = ?`,
				db1.DatabaseID, db1.DatabaseBranchID).Scan(&branchRefID)

			if err != nil {
				t.Fatalf("failed to get branch ref id: %v", err)
			}

			// Verify directories exist before cleanup
			if count := countQueryLogDirs(db1); count != 4 {
				t.Fatalf("Expected 4 query log directories before cleanup, got %d", count)
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

			err = app.CleanupQueryLogJob(ctx, data)

			if err != nil {
				t.Fatalf("CleanupQueryLogJob failed: %v", err)
			}

			// Verify only old directories were deleted (2 should remain)
			if count := countQueryLogDirs(db1); count != 2 {
				t.Errorf("Expected 2 query log directories after cleanup, got %d", count)
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

			err = app.CleanupQueryLogJob(ctx, data)

			if err != nil {
				t.Fatalf("CleanupQueryLogJob should not fail for missing directory: %v", err)
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

			err = app.CleanupQueryLogJob(ctx, data)

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

			err := app.CleanupQueryLogJob(ctx, data)

			if err == nil {
				t.Error("Expected error for invalid cutoff_timestamp type")
			}
		})

		t.Run("NoDirectoriesToCleanup", func(t *testing.T) {
			db1 := test.MockDatabase(app)

			// Create only recent query logs (within retention period)
			createMockQueryLogDirs(db1, 5)
			createMockQueryLogDirs(db1, 10)

			// Get branch ref ID
			var branchRefID int64
			err := sysDB.QueryRow(`SELECT id FROM database_branches WHERE database_id = ? AND database_branch_id = ?`,
				db1.DatabaseID, db1.DatabaseBranchID).Scan(&branchRefID)

			if err != nil {
				t.Fatalf("failed to get branch ref id: %v", err)
			}

			beforeCount := countQueryLogDirs(db1)

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

			err = app.CleanupQueryLogJob(ctx, data)

			if err != nil {
				t.Fatalf("CleanupQueryLogJob failed: %v", err)
			}

			// Verify no directories were deleted
			afterCount := countQueryLogDirs(db1)

			if afterCount != beforeCount {
				t.Errorf("Expected no directories to be deleted, before=%d after=%d", beforeCount, afterCount)
			}
		})
	})
}
