package server_test

import (
	"context"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/server"
)

func TestEnqueueIncrementalBackupCleanupJobs(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		sysDB, err := app.DatabaseManager.SystemDatabase().DB()

		if err != nil {
			t.Fatalf("failed to get system db: %v", err)
		}

		// Helper to count cleanup jobs for a specific branch
		countCleanupJobsForBranch := func(databaseID, branchID string) int {
			var count int

			err := sysDB.QueryRow(`
				SELECT COUNT(*) 
				FROM queued_jobs 
				WHERE name = 'CleanupIncrementalBackupJob' 
				AND json_extract(data, '$.database_id') = ?
				AND json_extract(data, '$.branch_id') = ?
				AND status IN ('pending', 'failed', 'processing')
			`, databaseID, branchID).Scan(&count)

			if err != nil {
				t.Fatalf("failed to count queued jobs: %v", err)
			}

			return count
		}

		t.Run("EnqueueForEnabledBranches", func(t *testing.T) {
			db1 := test.MockDatabase(app)
			db2 := test.MockDatabase(app)

			// Both should have incremental backups enabled by default
			ctx := context.Background()
			err := app.EnqueueIncrementalBackupCleanupJobs(ctx)

			if err != nil {
				t.Fatalf("EnqueueIncrementalBackupCleanupJobs failed: %v", err)
			}

			// Verify cleanup jobs were dispatched
			if countCleanupJobsForBranch(db1.DatabaseID, db1.DatabaseBranchID) != 1 {
				t.Error("Expected cleanup job for database 1")
			}

			if countCleanupJobsForBranch(db2.DatabaseID, db2.DatabaseBranchID) != 1 {
				t.Error("Expected cleanup job for database 2")
			}
		})

		t.Run("IgnoreDisabledBranches", func(t *testing.T) {
			db1 := test.MockDatabase(app)

			// Disable incremental backups
			var branchRefID int64

			err := sysDB.QueryRow(`SELECT id FROM database_branches WHERE database_id = ? AND database_branch_id = ?`,
				db1.DatabaseID, db1.DatabaseBranchID).Scan(&branchRefID)

			if err != nil {
				t.Fatalf("failed to get branch ref id: %v", err)
			}

			_, err = sysDB.Exec(`
				UPDATE database_branch_settings
				SET incremental_backups_enabled = 0
				WHERE database_branch_reference_id = ?
			`, branchRefID)

			if err != nil {
				t.Fatalf("failed to disable incremental backups: %v", err)
			}

			ctx := context.Background()
			err = app.EnqueueIncrementalBackupCleanupJobs(ctx)

			if err != nil {
				t.Fatalf("EnqueueIncrementalBackupCleanupJobs failed: %v", err)
			}

			// Should not have a cleanup job
			if countCleanupJobsForBranch(db1.DatabaseID, db1.DatabaseBranchID) != 0 {
				t.Error("Expected no cleanup job for disabled branch")
			}
		})

		t.Run("UniqueJobDispatch", func(t *testing.T) {
			db1 := test.MockDatabase(app)

			ctx := context.Background()

			// Dispatch twice
			err := app.EnqueueIncrementalBackupCleanupJobs(ctx)

			if err != nil {
				t.Fatalf("First enqueue failed: %v", err)
			}

			err = app.EnqueueIncrementalBackupCleanupJobs(ctx)

			if err != nil {
				t.Fatalf("Second enqueue failed: %v", err)
			}

			// Should still only have one job (due to unique key)
			if countCleanupJobsForBranch(db1.DatabaseID, db1.DatabaseBranchID) != 1 {
				t.Error("Expected only one cleanup job due to unique key")
			}
		})

		t.Run("Pagination", func(t *testing.T) {
			// Create 5 branches to test pagination
			dbs := make([]test.TestDatabase, 5)

			for i := 0; i < 5; i++ {
				dbs[i] = test.MockDatabase(app)
			}

			ctx := context.Background()
			err := app.EnqueueIncrementalBackupCleanupJobs(ctx)

			if err != nil {
				t.Fatalf("EnqueueIncrementalBackupCleanupJobs failed: %v", err)
			}

			// Verify all 5 got cleanup jobs
			for i, db := range dbs {
				if countCleanupJobsForBranch(db.DatabaseID, db.DatabaseBranchID) != 1 {
					t.Errorf("Expected cleanup job for database %d", i)
				}
			}
		})

		t.Run("ContextCancellation", func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel() // Cancel immediately

			err := app.EnqueueIncrementalBackupCleanupJobs(ctx)

			if err == nil {
				t.Error("Expected error with cancelled context, got nil")
			}
		})
	})
}
