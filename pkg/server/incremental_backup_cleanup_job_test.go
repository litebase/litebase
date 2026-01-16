package server_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/file"
	"github.com/litebase/litebase/pkg/server"
)

func TestCleanupIncrementalBackupJob(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		sysDB, err := app.DatabaseManager.SystemDatabase().DB()

		if err != nil {
			t.Fatalf("failed to get system db: %v", err)
		}

		// Helper to create mock incremental backup files (snapshots and rollback logs)
		createMockIncrementalBackups := func(db test.TestDatabase, daysAgo int) {
			branch, err := app.DatabaseManager.GetBranch(db.DatabaseID, db.DatabaseBranchID)

			if err != nil {
				t.Fatalf("failed to get database branch: %v", err)
			}

			resources := app.DatabaseManager.Resources(branch)
			tieredFS := resources.FileSystem().FileSystem()

			timestamp := time.Now().UTC().Add(-time.Duration(daysAgo) * 24 * time.Hour).UnixNano()

			// Create snapshot file
			snapshotDir := file.GetDatabaseSnapshotDirectory(db.DatabaseID, db.DatabaseBranchID)

			if err := tieredFS.MkdirAll(snapshotDir, 0750); err != nil {
				t.Fatalf("failed to create snapshot directory: %v", err)
			}

			snapshotPath := fmt.Sprintf("%s/%d", snapshotDir, timestamp)
			snapshotFile, err := tieredFS.Create(snapshotPath)

			if err != nil {
				t.Fatalf("failed to create snapshot file: %v", err)
			}

			if _, err := snapshotFile.Write([]byte("mock snapshot data")); err != nil {
				t.Fatalf("failed to write snapshot data: %v", err)
			}

			if err := snapshotFile.Close(); err != nil {
				t.Fatalf("failed to close snapshot file: %v", err)
			}

			// Create rollback log file
			rollbackDir := file.GetDatabaseRollbackDirectory(db.DatabaseID, db.DatabaseBranchID)

			if err := tieredFS.MkdirAll(rollbackDir, 0750); err != nil {
				t.Fatalf("failed to create rollback directory: %v", err)
			}

			rollbackPath := fmt.Sprintf("%s/%d", rollbackDir, timestamp)
			rollbackFile, err := tieredFS.Create(rollbackPath)

			if err != nil {
				t.Fatalf("failed to create rollback file: %v", err)
			}

			if _, err := rollbackFile.Write([]byte("mock rollback data")); err != nil {
				t.Fatalf("failed to write rollback data: %v", err)
			}

			if err := rollbackFile.Close(); err != nil {
				t.Fatalf("failed to close rollback file: %v", err)
			}
		}

		// Helper to count files in a directory
		countFilesInDir := func(db test.TestDatabase, dir string) int {
			branch, err := app.DatabaseManager.GetBranch(db.DatabaseID, db.DatabaseBranchID)

			if err != nil {
				return 0
			}

			resources := app.DatabaseManager.Resources(branch)
			tieredFS := resources.FileSystem().FileSystem()

			entries, err := tieredFS.ReadDir(dir)

			if err != nil {
				return 0
			}

			count := 0

			for _, entry := range entries {
				if !entry.IsDir() {
					count++
				}
			}

			return count
		}

		t.Run("SuccessfulCleanup", func(t *testing.T) {
			db1 := test.MockDatabase(app)

			// Create old backups (10 days old, should be cleaned with 7-day retention)
			createMockIncrementalBackups(db1, 10)

			// Create recent backups (3 days old, should be kept)
			createMockIncrementalBackups(db1, 3)

			// Get branch ref ID
			var branchRefID int64

			err := sysDB.QueryRow(`SELECT id FROM database_branches WHERE database_id = ? AND database_branch_id = ?`,
				db1.DatabaseID, db1.DatabaseBranchID).Scan(&branchRefID)

			if err != nil {
				t.Fatalf("failed to get branch ref id: %v", err)
			}

			// Verify files exist before cleanup
			snapshotDir := file.GetDatabaseSnapshotDirectory(db1.DatabaseID, db1.DatabaseBranchID)
			rollbackDir := file.GetDatabaseRollbackDirectory(db1.DatabaseID, db1.DatabaseBranchID)

			if countFilesInDir(db1, snapshotDir) != 2 {
				t.Fatal("Expected 2 snapshot files before cleanup")
			}

			if countFilesInDir(db1, rollbackDir) != 2 {
				t.Fatal("Expected 2 rollback files before cleanup")
			}

			// Run cleanup job with 7-day retention
			ctx := context.Background()
			cutoffTime := time.Now().UTC().Add(-7 * 24 * time.Hour)

			data := map[string]any{
				"database_id":      db1.DatabaseID,
				"branch_id":        db1.DatabaseBranchID,
				"cutoff_timestamp": float64(cutoffTime.UnixNano()),
				"retention_days":   float64(7),
				"branch_ref_id":    float64(branchRefID),
			}

			err = app.CleanupIncrementalBackupJob(ctx, data)

			if err != nil {
				t.Fatalf("CleanupIncrementalBackupJob failed: %v", err)
			}

			// Verify only old files were deleted (1 snapshot + 1 rollback should remain)
			if countFilesInDir(db1, snapshotDir) != 1 {
				t.Errorf("Expected 1 snapshot file after cleanup, got %d", countFilesInDir(db1, snapshotDir))
			}

			if countFilesInDir(db1, rollbackDir) != 1 {
				t.Errorf("Expected 1 rollback file after cleanup, got %d", countFilesInDir(db1, rollbackDir))
			}

			// Verify incremental_backups_cleaned_at was updated
			var cleanedAt sql.NullInt64

			err = sysDB.QueryRow(`
			SELECT incremental_backups_cleaned_at 
			FROM database_branch_settings 
			WHERE database_branch_reference_id = ?
		`, branchRefID).Scan(&cleanedAt)

			if err != nil {
				t.Fatalf("failed to query incremental_backups_cleaned_at: %v", err)
			}

			if !cleanedAt.Valid {
				t.Error("Expected incremental_backups_cleaned_at to be set")
			}
		})

		t.Run("MissingDirectories", func(t *testing.T) {
			db1 := test.MockDatabase(app)

			// Get branch ref ID
			var branchRefID int64

			err := sysDB.QueryRow(`SELECT id FROM database_branches WHERE database_id = ? AND database_branch_id = ?`,
				db1.DatabaseID, db1.DatabaseBranchID).Scan(&branchRefID)

			if err != nil {
				t.Fatalf("failed to get branch ref id: %v", err)
			}

			// Run cleanup (directories don't exist, should not error)
			ctx := context.Background()
			cutoffTime := time.Now().UTC().Add(-7 * 24 * time.Hour)

			data := map[string]any{
				"database_id":      db1.DatabaseID,
				"branch_id":        db1.DatabaseBranchID,
				"cutoff_timestamp": float64(cutoffTime.UnixNano()),
				"retention_days":   float64(7),
				"branch_ref_id":    float64(branchRefID),
			}

			err = app.CleanupIncrementalBackupJob(ctx, data)

			if err != nil {
				t.Fatalf("CleanupIncrementalBackupJob should not fail for missing directories: %v", err)
			}
		})

		t.Run("MissingDatabaseID", func(t *testing.T) {
			ctx := context.Background()

			data := map[string]any{
				"branch_id":        "test-branch",
				"cutoff_timestamp": float64(123456),
				"retention_days":   float64(7),
				"branch_ref_id":    float64(1),
			}

			err := app.CleanupIncrementalBackupJob(ctx, data)

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
				"retention_days":   float64(7),
				"branch_ref_id":    float64(1),
			}

			err := app.CleanupIncrementalBackupJob(ctx, data)

			if err == nil {
				t.Error("Expected error for invalid cutoff_timestamp type")
			}
		})
	})
}
