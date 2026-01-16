package server_test

import (
	"context"
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/server"
)

func TestEnqueueBackupCleanupJobs(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		sysDB, err := app.DatabaseManager.SystemDatabase().DB()

		if err != nil {
			t.Fatalf("failed to get system db: %v", err)
		}

		// Helper to count cleanup jobs for a specific backup
		countCleanupJobsForBackup := func(backupID int64) int {
			var count int

			err := sysDB.QueryRow(`
				SELECT COUNT(*) 
				FROM queued_jobs 
				WHERE name = 'CleanupBackupJob' 
				AND json_extract(data, '$.backup_id') = ?
				AND status IN ('pending', 'failed', 'processing')
			`, backupID).Scan(&count)

			if err != nil {
				t.Fatalf("failed to count queued jobs: %v", err)
			}

			return count
		}

		// Helper to create a mock backup record
		createMockBackup := func(db test.TestDatabase, createdDaysAgo int) int64 {
			// Get branch reference ID
			var branchRefID int64

			err := sysDB.QueryRow(`SELECT id FROM database_branches WHERE database_id = ? AND database_branch_id = ?`,
				db.DatabaseID, db.DatabaseBranchID).Scan(&branchRefID)
			if err != nil {
				t.Fatalf("failed to get branch ref id: %v", err)
			}

			// Get database reference ID
			var dbRefID int64

			err = sysDB.QueryRow(`SELECT id FROM databases WHERE database_id = ?`, db.DatabaseID).Scan(&dbRefID)

			if err != nil {
				t.Fatalf("failed to get db ref id: %v", err)
			}

			createdAt := time.Now().UTC().Add(-time.Duration(createdDaysAgo) * 24 * time.Hour)
			restorePointTimestamp := createdAt.UnixNano()

			result, err := sysDB.Exec(`
				INSERT INTO database_backups (
					database_reference_id,
					database_branch_reference_id,
					database_id,
					database_branch_id,
					restore_point_timestamp,
					restore_point_page_count,
					size,
					created_at
				) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
			`, dbRefID, branchRefID, db.DatabaseID, db.DatabaseBranchID,
				restorePointTimestamp, 100, 1024, createdAt.Format(time.RFC3339))

			if err != nil {
				t.Fatalf("failed to create mock backup: %v", err)
			}

			backupID, err := result.LastInsertId()
			if err != nil {
				t.Fatalf("failed to get backup id: %v", err)
			}

			return backupID
		}

		t.Run("EnqueueExpiredBackups", func(t *testing.T) {
			// Create 3 databases with different backup ages
			db1 := test.MockDatabase(app)
			db2 := test.MockDatabase(app)
			db3 := test.MockDatabase(app)

			// Get default retention days (30 days from NewDefaultBranchSettings)
			// Create backups: one expired (40 days old), one borderline (31 days old - should be expired), one fresh (20 days)
			backup1 := createMockBackup(db1, 40) // Expired
			backup2 := createMockBackup(db2, 31) // Borderline (>30 days, should be expired)
			backup3 := createMockBackup(db3, 20) // Not expired

			// Run enqueue
			ctx := context.Background()
			err := app.EnqueueBackupCleanupJobs(ctx)

			if err != nil {
				t.Fatalf("EnqueueBackupCleanupJobs failed: %v", err)
			}

			// Verify expired backups have jobs
			if countCleanupJobsForBackup(backup1) != 1 {
				t.Error("Expected cleanup job for 40-day-old backup")
			}

			if countCleanupJobsForBackup(backup2) != 1 {
				t.Error("Expected cleanup job for 31-day-old backup")
			}

			// Verify fresh backup does NOT have a job
			if countCleanupJobsForBackup(backup3) != 0 {
				t.Error("Expected no cleanup job for 20-day-old backup")
			}
		})

		t.Run("IgnoreNonExpiredBackups", func(t *testing.T) {
			db1 := test.MockDatabase(app)

			// Create a fresh backup (1 day old)
			backup1 := createMockBackup(db1, 1)

			// Run enqueue
			ctx := context.Background()
			err := app.EnqueueBackupCleanupJobs(ctx)

			if err != nil {
				t.Fatalf("EnqueueBackupCleanupJobs failed: %v", err)
			}

			// Verify no job was created
			if countCleanupJobsForBackup(backup1) != 0 {
				t.Error("Expected no cleanup job for fresh backup")
			}
		})

		t.Run("CustomRetentionDays", func(t *testing.T) {
			db1 := test.MockDatabase(app)

			// Update retention days to 7 days
			d1, _ := app.DatabaseManager.Get(db1.DatabaseID)
			b1, _ := d1.PrimaryBranch()
			settings, _ := b1.GetBranchSettings()
			settings.BackupsRetentionDays = 7
			err := b1.UpdateBranchSettings(settings)

			if err != nil {
				t.Fatalf("failed to update settings: %v", err)
			}

			// Create a 10-day-old backup (expired with 7-day retention)
			backup1 := createMockBackup(db1, 10)

			// Run enqueue
			ctx := context.Background()
			err = app.EnqueueBackupCleanupJobs(ctx)

			if err != nil {
				t.Fatalf("EnqueueBackupCleanupJobs failed: %v", err)
			}

			// Verify job was created
			if countCleanupJobsForBackup(backup1) != 1 {
				t.Error("Expected cleanup job for backup exceeding custom retention")
			}
		})

		t.Run("UniqueJobDispatch", func(t *testing.T) {
			db1 := test.MockDatabase(app)

			// Create an expired backup
			backup1 := createMockBackup(db1, 40)

			// Run enqueue twice
			ctx := context.Background()
			err := app.EnqueueBackupCleanupJobs(ctx)

			if err != nil {
				t.Fatalf("First EnqueueBackupCleanupJobs failed: %v", err)
			}

			err = app.EnqueueBackupCleanupJobs(ctx)

			if err != nil {
				t.Fatalf("Second EnqueueBackupCleanupJobs failed: %v", err)
			}

			// Should only have 1 job (unique key prevents duplicates)
			if countCleanupJobsForBackup(backup1) != 1 {
				t.Error("Expected only 1 cleanup job due to unique key constraint")
			}
		})

		t.Run("Pagination", func(t *testing.T) {
			// Create multiple databases with expired backups
			numBackups := 5
			databases := make([]test.TestDatabase, numBackups)
			backupIDs := make([]int64, numBackups)

			for i := 0; i < numBackups; i++ {
				db := test.MockDatabase(app)
				databases[i] = db
				backupIDs[i] = createMockBackup(db, 40) // All expired
			}

			// Run enqueue
			ctx := context.Background()
			err := app.EnqueueBackupCleanupJobs(ctx)

			if err != nil {
				t.Fatalf("EnqueueBackupCleanupJobs failed: %v", err)
			}

			// Verify each backup has a cleanup job
			for i, backupID := range backupIDs {
				if countCleanupJobsForBackup(backupID) != 1 {
					t.Errorf("Expected cleanup job for backup %d", i+1)
				}
			}
		})

		t.Run("ContextCancellation", func(t *testing.T) {
			db1 := test.MockDatabase(app)
			createMockBackup(db1, 40)

			// Create cancelled context
			ctx, cancel := context.WithCancel(context.Background())
			cancel()

			// Should return error
			err := app.EnqueueBackupCleanupJobs(ctx)

			if err == nil {
				t.Error("Expected error with cancelled context, got nil")
			}
		})
	})
}
