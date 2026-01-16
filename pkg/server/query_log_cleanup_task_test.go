package server_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/server"
)

func TestEnqueueQueryLogCleanupJobs(t *testing.T) {
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
				WHERE name = 'CleanupQueryLogJob' 
				AND key = ?
			`, fmt.Sprintf("cleanup-query-log:%s:%s", databaseID, branchID)).Scan(&count)

			if err != nil {
				t.Fatalf("failed to count jobs: %v", err)
			}

			return count
		}

		t.Run("EnqueueForAllBranches", func(t *testing.T) {
			// Clean up any existing jobs
			_, err := sysDB.Exec(`DELETE FROM queued_jobs WHERE name = 'CleanupQueryLogJob'`)

			if err != nil {
				t.Fatalf("failed to clean up existing jobs: %v", err)
			}

			// Create test databases
			db1 := test.MockDatabase(app)
			db2 := test.MockDatabase(app)

			// Run the task
			ctx := context.Background()
			err = app.EnqueueQueryLogCleanupJobs(ctx)

			if err != nil {
				t.Fatalf("EnqueueQueryLogCleanupJobs failed: %v", err)
			}

			// Verify jobs were created
			if count := countCleanupJobsForBranch(db1.DatabaseID, db1.DatabaseBranchID); count != 1 {
				t.Errorf("Expected 1 cleanup job for db1, got %d", count)
			}

			if count := countCleanupJobsForBranch(db2.DatabaseID, db2.DatabaseBranchID); count != 1 {
				t.Errorf("Expected 1 cleanup job for db2, got %d", count)
			}
		})

		t.Run("UniqueJobDispatch", func(t *testing.T) {
			// Clean up any existing jobs
			_, err := sysDB.Exec(`DELETE FROM queued_jobs WHERE name = 'CleanupQueryLogJob'`)

			if err != nil {
				t.Fatalf("failed to clean up existing jobs: %v", err)
			}

			db1 := test.MockDatabase(app)

			// Run the task twice
			ctx := context.Background()

			err = app.EnqueueQueryLogCleanupJobs(ctx)

			if err != nil {
				t.Fatalf("First EnqueueQueryLogCleanupJobs failed: %v", err)
			}

			err = app.EnqueueQueryLogCleanupJobs(ctx)

			if err != nil {
				t.Fatalf("Second EnqueueQueryLogCleanupJobs failed: %v", err)
			}

			// Should still only have 1 job (unique key prevents duplicates)
			if count := countCleanupJobsForBranch(db1.DatabaseID, db1.DatabaseBranchID); count != 1 {
				t.Errorf("Expected 1 unique job after running task twice, got %d", count)
			}
		})

		t.Run("Pagination", func(t *testing.T) {
			// Clean up any existing jobs
			_, err := sysDB.Exec(`DELETE FROM queued_jobs WHERE name = 'CleanupQueryLogJob'`)

			if err != nil {
				t.Fatalf("failed to clean up existing jobs: %v", err)
			}

			// Create multiple databases to test pagination
			dbs := make([]test.TestDatabase, 5)

			for i := range dbs {
				dbs[i] = test.MockDatabase(app)
			}

			// Run the task
			ctx := context.Background()
			err = app.EnqueueQueryLogCleanupJobs(ctx)

			if err != nil {
				t.Fatalf("EnqueueQueryLogCleanupJobs failed: %v", err)
			}

			// Verify all databases got cleanup jobs
			for i, db := range dbs {
				if countCleanupJobsForBranch(db.DatabaseID, db.DatabaseBranchID) != 1 {
					t.Errorf("Expected cleanup job for database %d", i)
				}
			}
		})

		t.Run("ContextCancellation", func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel() // Cancel immediately

			err = app.EnqueueQueryLogCleanupJobs(ctx)

			if err == nil {
				t.Error("Expected error with cancelled context, got nil")
			}
		})
	})
}
