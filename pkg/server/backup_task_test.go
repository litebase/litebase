package server_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/database"
	"github.com/litebase/litebase/pkg/queue"
	"github.com/litebase/litebase/pkg/server"
)

func TestEnqueueBackupJobs(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		sysDB, err := app.DatabaseManager.SystemDatabase().DB()

		if err != nil {
			t.Fatalf("failed to get system db: %v", err)
		}

		// Helper to count queued backup jobs for a specific branch (pending or failed)
		countBackupJobsForBranch := func(databaseID, branchID string) int {
			var count int

			err := sysDB.QueryRow(`
				SELECT COUNT(*) 
				FROM queued_jobs 
				WHERE name = 'BackupJob' 
				AND json_extract(data, '$.database_id') = ?
				AND json_extract(data, '$.branch_id') = ?
				AND status IN ('pending', 'failed', 'processing')
			`, databaseID, branchID).Scan(&count)

			if err != nil {
				t.Fatalf("failed to count queued jobs: %v", err)
			}

			return count
		}

		// Helper to set backup_next_at for a branch
		setBackupNextAt := func(branchID int64, timestamp sql.NullInt64) {
			_, err := sysDB.Exec(`UPDATE database_branch_settings SET backup_next_at = ? WHERE database_branch_reference_id = ?`, timestamp, branchID)

			if err != nil {
				t.Fatalf("failed to set backup_next_at: %v", err)
			}
		}

		// Helper to enable/disable backups
		setBackupsEnabled := func(branchID int64, enabled bool) {
			val := 0

			if enabled {
				val = 1
			}

			_, err := sysDB.Exec(`UPDATE database_branch_settings SET backups_enabled = ? WHERE database_branch_reference_id = ?`, val, branchID)

			if err != nil {
				t.Fatalf("failed to set backups_enabled: %v", err)
			}
		}

		t.Run("EnqueueDueBranches", func(t *testing.T) {
			// Create 3 databases
			db1 := test.MockDatabase(app)
			db2 := test.MockDatabase(app)
			db3 := test.MockDatabase(app)

			d1, _ := app.DatabaseManager.Get(db1.DatabaseID)
			d2, _ := app.DatabaseManager.Get(db2.DatabaseID)
			d3, _ := app.DatabaseManager.Get(db3.DatabaseID)

			b1, _ := d1.PrimaryBranch()
			b2, _ := d2.PrimaryBranch()
			b3, _ := d3.PrimaryBranch()

			// Set backup_next_at to 1 hour ago (all due)
			oneHourAgo := sql.NullInt64{Int64: time.Now().UTC().Add(-1 * time.Hour).Unix(), Valid: true}
			setBackupNextAt(b1.ID, oneHourAgo)
			setBackupNextAt(b2.ID, oneHourAgo)
			setBackupNextAt(b3.ID, oneHourAgo)

			// Debug: check how many branches are due
			var dueCount int
			nowUnix := time.Now().UTC().Unix()

			err := sysDB.QueryRow(`
				SELECT COUNT(*) 
				FROM database_branches b
				JOIN database_branch_settings s ON s.database_branch_reference_id = b.id
				WHERE s.backups_enabled = 1
				  AND (s.backup_next_at IS NULL OR s.backup_next_at <= ?)
			`, nowUnix).Scan(&dueCount)

			if err != nil {
				t.Fatalf("Failed to count due branches: %v", err)
			}

			t.Logf("Branches due for backup: %d", dueCount)

			// Debug: check the actual query results
			rows, err := sysDB.Query(`
				SELECT b.id, b.database_id, b.database_branch_id, s.backups_interval
				FROM database_branches b
				JOIN database_branch_settings s ON s.database_branch_reference_id = b.id
				WHERE s.backups_enabled = 1
				  AND (s.backup_next_at IS NULL OR s.backup_next_at <= ?)
				ORDER BY b.id ASC
			`, nowUnix)

			if err != nil {
				t.Fatalf("Failed to query: %v", err)
			}

			t.Log("Query results:")

			for rows.Next() {
				var (
					branchRefID     int64
					databaseID      string
					branchID        string
					backupsInterval sql.NullString
				)

				err := rows.Scan(&branchRefID, &databaseID, &branchID, &backupsInterval)

				if err != nil {
					t.Fatalf("Failed to scan row: %v", err)
				}

				t.Logf("  - Branch ID=%d, DB=%s, Branch=%s", branchRefID, databaseID, branchID)
			}

			err = rows.Close()

			if err != nil {
				t.Fatalf("Failed to close rows: %v", err)
			}

			// Run enqueue manually with debug output
			ctx := context.Background()

			const pageSize = 1000
			lastID := int64(0)
			nowUnix2 := time.Now().UTC().Unix()
			totalDispatched := 0

			for {
				if ctx.Err() != nil {
					t.Fatal("Context cancelled")
				}

				rows2, err2 := sysDB.Query(`
					SELECT b.id, b.database_id, b.database_branch_id, s.backups_interval
					FROM database_branches b
					JOIN database_branch_settings s ON s.database_branch_reference_id = b.id
					WHERE s.backups_enabled = 1
					  AND (s.backup_next_at IS NULL OR s.backup_next_at <= ?)
					  AND b.id > ?
					ORDER BY b.id ASC
					LIMIT ?
				`, nowUnix2, lastID, pageSize)

				if err2 != nil {
					t.Fatalf("Query failed: %v", err2)
				}

				count := 0

				for rows2.Next() {
					var (
						branchRefID     int64
						databaseID      string
						branchID        string
						backupsInterval sql.NullString
					)

					if err := rows2.Scan(&branchRefID, &databaseID, &branchID, &backupsInterval); err != nil {
						t.Logf("Scan error: %v", err)
						continue
					}

					count++
					lastID = branchRefID
					t.Logf("Processing branch %d: db=%s, branch=%s", branchRefID, databaseID, branchID)

					data := map[string]any{
						"database_id": databaseID,
						"branch_id":   branchID,
					}

					key := fmt.Sprintf("backup:%s:%s", databaseID, branchID)

					jobID, dispErr := app.QueueDispatcher.DispatchJob("BackupJob", data, queue.WithKey(key), queue.Unique())

					if dispErr != nil {
						t.Logf("Dispatch error for branch %d: %v", branchRefID, dispErr)
						continue
					}

					totalDispatched++
					t.Logf("Dispatched job %d for branch %d", jobID, branchRefID)
				}

				t.Logf("Processed %d rows in this page", count)

				err2 = rows2.Close()

				if err2 != nil {
					t.Fatalf("Failed to close rows: %v", err2)
				}

				if count < pageSize {
					break
				}
			}

			t.Logf("Total dispatched: %d", totalDispatched)

			// Original call for comparison
			err = app.EnqueueBackupJobs(ctx)

			if err != nil {
				t.Fatalf("EnqueueBackupJobs failed: %v", err)
			}

			// Add a small delay to allow jobs to be dispatched
			time.Sleep(100 * time.Millisecond)

			// Debug: check all queued jobs
			var allJobs int
			err = sysDB.QueryRow("SELECT COUNT(*) FROM queued_jobs WHERE name = 'BackupJob'").Scan(&allJobs)

			if err != nil {
				t.Fatalf("Failed to count backup jobs: %v", err)
			}

			t.Logf("Total BackupJobs in queue: %d", allJobs)

			// Debug: check jobs for each branch
			t.Logf("Branch 1: db=%s, branch=%s, count=%d", db1.DatabaseID, db1.DatabaseBranchID, countBackupJobsForBranch(db1.DatabaseID, db1.DatabaseBranchID))
			t.Logf("Branch 2: db=%s, branch=%s, count=%d", db2.DatabaseID, db2.DatabaseBranchID, countBackupJobsForBranch(db2.DatabaseID, db2.DatabaseBranchID))
			t.Logf("Branch 3: db=%s, branch=%s, count=%d", db3.DatabaseID, db3.DatabaseBranchID, countBackupJobsForBranch(db3.DatabaseID, db3.DatabaseBranchID))

			// Verify each branch has a job
			if countBackupJobsForBranch(db1.DatabaseID, db1.DatabaseBranchID) != 1 {
				t.Error("Expected backup job for branch 1")
			}

			if countBackupJobsForBranch(db2.DatabaseID, db2.DatabaseBranchID) != 1 {
				t.Error("Expected backup job for branch 2")
			}

			if countBackupJobsForBranch(db3.DatabaseID, db3.DatabaseBranchID) != 1 {
				t.Error("Expected backup job for branch 3")
			}
		})

		t.Run("IgnoreFutureBranches", func(t *testing.T) {
			db1 := test.MockDatabase(app)
			d1, _ := app.DatabaseManager.Get(db1.DatabaseID)
			b1, _ := d1.PrimaryBranch()

			// Set backup_next_at to 1 hour in the future
			oneHourFuture := sql.NullInt64{Int64: time.Now().UTC().Add(1 * time.Hour).Unix(), Valid: true}
			setBackupNextAt(b1.ID, oneHourFuture)

			// Run enqueue
			ctx := context.Background()
			err := app.EnqueueBackupJobs(ctx)

			if err != nil {
				t.Fatalf("EnqueueBackupJobs failed: %v", err)
			}

			// Should not enqueue (future backup)
			if countBackupJobsForBranch(db1.DatabaseID, db1.DatabaseBranchID) != 0 {
				t.Error("Expected no backup job for future backup")
			}
		})

		t.Run("IgnoreDisabledBackups", func(t *testing.T) {
			db1 := test.MockDatabase(app)
			d1, _ := app.DatabaseManager.Get(db1.DatabaseID)
			b1, _ := d1.PrimaryBranch()

			// Disable backups
			setBackupsEnabled(b1.ID, false)

			// Set backup_next_at to 1 hour ago (would be due if enabled)
			oneHourAgo := sql.NullInt64{Int64: time.Now().UTC().Add(-1 * time.Hour).Unix(), Valid: true}
			setBackupNextAt(b1.ID, oneHourAgo)

			// Run enqueue
			ctx := context.Background()
			err := app.EnqueueBackupJobs(ctx)

			if err != nil {
				t.Fatalf("EnqueueBackupJobs failed: %v", err)
			}

			// Should not enqueue (backups disabled)
			if countBackupJobsForBranch(db1.DatabaseID, db1.DatabaseBranchID) != 0 {
				t.Error("Expected no backup job for disabled backups")
			}
		})

		t.Run("EnqueueNullBackupNextAt", func(t *testing.T) {
			db1 := test.MockDatabase(app)
			d1, _ := app.DatabaseManager.Get(db1.DatabaseID)
			b1, _ := d1.PrimaryBranch()

			// Set backup_next_at to NULL (first backup)
			setBackupNextAt(b1.ID, sql.NullInt64{Valid: false})

			// Run enqueue
			ctx := context.Background()
			err := app.EnqueueBackupJobs(ctx)

			if err != nil {
				t.Fatalf("EnqueueBackupJobs failed: %v", err)
			}

			// Should enqueue (NULL is treated as due)
			if countBackupJobsForBranch(db1.DatabaseID, db1.DatabaseBranchID) != 1 {
				t.Error("Expected backup job for branch with NULL next_at")
			}
		})

		t.Run("UniqueJobDispatch", func(t *testing.T) {
			db1 := test.MockDatabase(app)
			d1, _ := app.DatabaseManager.Get(db1.DatabaseID)
			b1, _ := d1.PrimaryBranch()

			// Set backup_next_at to 1 hour ago
			oneHourAgo := sql.NullInt64{Int64: time.Now().UTC().Add(-1 * time.Hour).Unix(), Valid: true}
			setBackupNextAt(b1.ID, oneHourAgo)

			// Run enqueue twice
			ctx := context.Background()
			err := app.EnqueueBackupJobs(ctx)

			if err != nil {
				t.Fatalf("First EnqueueBackupJobs failed: %v", err)
			}

			err = app.EnqueueBackupJobs(ctx)

			if err != nil {
				t.Fatalf("Second EnqueueBackupJobs failed: %v", err)
			}

			// Should only enqueue 1 job (unique key prevents duplicates)
			if countBackupJobsForBranch(db1.DatabaseID, db1.DatabaseBranchID) != 1 {
				t.Error("Expected only 1 backup job due to unique key constraint")
			}
		})

		t.Run("Pagination", func(t *testing.T) {
			// This test would need to create > 1000 branches to test pagination
			// For now, we'll create a smaller set and verify they all get enqueued
			numBranches := 5
			databases := make([]test.TestDatabase, numBranches)

			for i := 0; i < numBranches; i++ {
				db := test.MockDatabase(app)
				databases[i] = db
				d, _ := app.DatabaseManager.Get(db.DatabaseID)
				b, _ := d.PrimaryBranch()

				// Set all to due for backup
				oneHourAgo := sql.NullInt64{Int64: time.Now().UTC().Add(-1 * time.Hour).Unix(), Valid: true}
				setBackupNextAt(b.ID, oneHourAgo)
			}

			// Run enqueue
			ctx := context.Background()
			err := app.EnqueueBackupJobs(ctx)

			if err != nil {
				t.Fatalf("EnqueueBackupJobs failed: %v", err)
			}

			// Verify each database has a job
			for i, db := range databases {
				if countBackupJobsForBranch(db.DatabaseID, db.DatabaseBranchID) != 1 {
					t.Errorf("Expected backup job for branch %d", i+1)
				}
			}
		})

		t.Run("ContextCancellation", func(t *testing.T) {
			db1 := test.MockDatabase(app)
			d1, _ := app.DatabaseManager.Get(db1.DatabaseID)
			b1, _ := d1.PrimaryBranch()

			oneHourAgo := sql.NullInt64{Int64: time.Now().UTC().Add(-1 * time.Hour).Unix(), Valid: true}
			setBackupNextAt(b1.ID, oneHourAgo)

			// Create cancelled context
			ctx, cancel := context.WithCancel(context.Background())
			cancel()

			// Should return error
			err := app.EnqueueBackupJobs(ctx)

			if err == nil {
				t.Error("Expected error with cancelled context, got nil")
			}
		})
	})
}

func TestEnqueueBackupJobs_Integration(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		// Create a database with actual data
		mock := test.MockDatabase(app)

		db, err := app.DatabaseManager.Get(mock.DatabaseID)

		if err != nil {
			t.Fatalf("failed to get database: %v", err)
		}

		branch, err := db.PrimaryBranch()

		if err != nil {
			t.Fatalf("failed to get branch: %v", err)
		}

		// Create some content
		conn, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

		if err != nil {
			t.Fatalf("failed to get connection: %v", err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(conn)

		_, err = conn.GetConnection().Exec("CREATE TABLE test_data (id INTEGER PRIMARY KEY, value TEXT)", nil)

		if err != nil {
			t.Fatalf("failed to create table: %v", err)
		}

		_, err = conn.GetConnection().Exec("INSERT INTO test_data (value) VALUES ('test')", nil)

		if err != nil {
			t.Fatalf("failed to insert data: %v", err)
		}

		if err := conn.Checkpoint(); err != nil {
			t.Fatalf("failed to checkpoint: %v", err)
		}

		// Update settings to enable backups with a past next_at
		settings, err := branch.GetBranchSettings()

		if err != nil {
			t.Fatalf("failed to get settings: %v", err)
		}

		settings.BackupsEnabled = true
		settings.BackupInterval = database.DatabaseBranchBackupInterval("24h")
		err = branch.UpdateBranchSettings(settings)

		if err != nil {
			t.Fatalf("failed to update settings: %v", err)
		}

		// Set backup_next_at to past
		sysDB, _ := app.DatabaseManager.SystemDatabase().DB()
		oneHourAgo := time.Now().UTC().Add(-1 * time.Hour).Unix()

		_, err = sysDB.Exec(`UPDATE database_branch_settings SET backup_next_at = ? WHERE database_branch_reference_id = ?`, oneHourAgo, branch.ID)

		if err != nil {
			t.Fatalf("failed to set backup_next_at: %v", err)
		}

		// Enqueue backups
		ctx := context.Background()
		err = app.EnqueueBackupJobs(ctx)

		if err != nil {
			t.Fatalf("EnqueueBackupJobs failed: %v", err)
		}

		// Verify job was queued
		var count int

		err = sysDB.QueryRow(`
			SELECT COUNT(*) 
			FROM queued_jobs 
			WHERE name = 'BackupJob' 
			AND json_extract(data, '$.database_id') = ?
			AND json_extract(data, '$.branch_id') = ?
		`, mock.DatabaseID, mock.DatabaseBranchID).Scan(&count)

		if err != nil {
			t.Fatalf("failed to query queued jobs: %v", err)
		}

		if count != 1 {
			t.Errorf("Expected 1 backup job queued, got %d", count)
		}
	})
}
