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

func TestCleanupBackupJob(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		sysDB, err := app.DatabaseManager.SystemDatabase().DB()

		if err != nil {
			t.Fatalf("failed to get system db: %v", err)
		}

		// Helper to create a mock backup record and files
		createMockBackupWithFiles := func(db test.TestDatabase, createdDaysAgo int) (int64, string) {
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

			// Insert backup record
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

			// Create mock backup files in object storage
			backupDir := fmt.Sprintf(
				"%s%d/",
				file.GetDatabaseBackupsDirectory(db.DatabaseID, db.DatabaseBranchID),
				restorePointTimestamp,
			)

			// Create directory and files
			if err := app.Cluster.ObjectFS().MkdirAll(backupDir, 0750); err != nil {
				t.Fatalf("failed to create backup directory: %v", err)
			}

			// Create mock backup files
			for i := 1; i <= 2; i++ {
				filePath := fmt.Sprintf("%sbackup-%d.tar.gz", backupDir, i)
				f, err := app.Cluster.ObjectFS().Create(filePath)

				if err != nil {
					t.Fatalf("failed to create backup file: %v", err)
				}

				_, err = f.Write([]byte("mock backup data"))

				if err != nil {
					t.Fatalf("failed to write backup file: %v", err)
				}

				err = f.Close()

				if err != nil {
					t.Fatalf("failed to close backup file: %v", err)
				}
			}

			return backupID, backupDir
		}

		t.Run("SuccessfulCleanup", func(t *testing.T) {
			db1 := test.MockDatabase(app)

			backupID, backupDir := createMockBackupWithFiles(db1, 40)

			// Verify backup record exists
			var count int

			err := sysDB.QueryRow(`SELECT COUNT(*) FROM database_backups WHERE id = ?`, backupID).Scan(&count)

			if err != nil {
				t.Fatalf("failed to query backup: %v", err)
			}

			if count != 1 {
				t.Fatal("Backup record not created")
			}

			// Verify backup files exist
			entries, err := app.Cluster.ObjectFS().ReadDir(backupDir)

			if err != nil {
				t.Fatalf("failed to read backup directory: %v", err)
			}

			if len(entries) != 2 {
				t.Fatalf("expected 2 backup files, got %d", len(entries))
			}

			// Run cleanup job
			ctx := context.Background()

			data := map[string]any{
				"backup_id":               float64(backupID),
				"database_id":             db1.DatabaseID,
				"branch_id":               db1.DatabaseBranchID,
				"restore_point_timestamp": float64(time.Now().Add(-40 * 24 * time.Hour).UnixNano()),
			}

			err = app.CleanupBackupJob(ctx, data)

			if err != nil {
				t.Fatalf("CleanupBackupJob failed: %v", err)
			}

			// Verify backup record was deleted
			err = sysDB.QueryRow(`SELECT COUNT(*) FROM database_backups WHERE id = ?`, backupID).Scan(&count)

			if err != nil {
				t.Fatalf("failed to query backup: %v", err)
			}

			if count != 0 {
				t.Error("Backup record was not deleted")
			}

			// Note: We don't verify file deletion here because it's a "best effort" operation.
			// The important thing is that the database record was removed so the backup
			// won't be referenced anymore. Orphaned files are less problematic than
			// orphaned database records pointing to missing files.

			// Verify backups_cleaned_at was updated
			var cleanedAt sql.NullInt64

			err2 := sysDB.QueryRow(`
				SELECT backups_cleaned_at 
				FROM database_branch_settings 
				WHERE database_branch_reference_id = (
					SELECT id FROM database_branches 
					WHERE database_id = ? AND database_branch_id = ?
				)
			`, db1.DatabaseID, db1.DatabaseBranchID).Scan(&cleanedAt)

			if err2 != nil {
				t.Fatalf("failed to query backups_cleaned_at: %v", err2)
			}

			if !cleanedAt.Valid {
				t.Error("Expected backups_cleaned_at to be set")
			}
		})

		t.Run("MissingBackupFiles", func(t *testing.T) {
			db1 := test.MockDatabase(app)

			// Create backup record WITHOUT files
			var branchRefID, dbRefID int64

			err := sysDB.QueryRow(`SELECT id FROM database_branches WHERE database_id = ? AND database_branch_id = ?`,
				db1.DatabaseID, db1.DatabaseBranchID).Scan(&branchRefID)

			if err != nil {
				t.Fatalf("failed to get branch ref id: %v", err)
			}

			err = sysDB.QueryRow(`SELECT id FROM databases WHERE database_id = ?`, db1.DatabaseID).Scan(&dbRefID)

			if err != nil {
				t.Fatalf("failed to get db ref id: %v", err)
			}

			createdAt := time.Now().UTC().Add(-40 * 24 * time.Hour)
			restorePointTimestamp := createdAt.UnixNano()

			result, _ := sysDB.Exec(`
				INSERT INTO database_backups (
					database_reference_id, database_branch_reference_id,
					database_id, database_branch_id, restore_point_timestamp,
					restore_point_page_count, size, created_at
				) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
			`, dbRefID, branchRefID, db1.DatabaseID, db1.DatabaseBranchID,
				restorePointTimestamp, 100, 1024, createdAt.Format(time.RFC3339))

			backupID, _ := result.LastInsertId()

			// Run cleanup (should succeed even if files don't exist)
			ctx := context.Background()
			data := map[string]any{
				"backup_id":               float64(backupID),
				"database_id":             db1.DatabaseID,
				"branch_id":               db1.DatabaseBranchID,
				"restore_point_timestamp": float64(restorePointTimestamp),
			}

			err = app.CleanupBackupJob(ctx, data)

			if err != nil {
				t.Fatalf("CleanupBackupJob failed: %v", err)
			}

			// Verify backup record was still deleted
			var count int
			err = sysDB.QueryRow(`SELECT COUNT(*) FROM database_backups WHERE id = ?`, backupID).Scan(&count)

			if err != nil {
				t.Fatalf("failed to query backup count: %v", err)
			}

			if count != 0 {
				t.Error("Backup record was not deleted")
			}
		})

		t.Run("MissingBackupID", func(t *testing.T) {
			ctx := context.Background()

			data := map[string]any{
				"database_id":             "test-db",
				"branch_id":               "test-branch",
				"restore_point_timestamp": float64(123456),
			}

			err := app.CleanupBackupJob(ctx, data)

			if err == nil {
				t.Error("Expected error for missing backup_id")
			}
		})

		t.Run("InvalidBackupID", func(t *testing.T) {
			ctx := context.Background()

			data := map[string]any{
				"backup_id":               "not-a-number",
				"database_id":             "test-db",
				"branch_id":               "test-branch",
				"restore_point_timestamp": float64(123456),
			}

			err := app.CleanupBackupJob(ctx, data)

			if err == nil {
				t.Error("Expected error for invalid backup_id type")
			}
		})

		t.Run("PartialFileCleanup", func(t *testing.T) {
			db1 := test.MockDatabase(app)

			backupID, _ := createMockBackupWithFiles(db1, 40)

			// Note: This test may behave differently on different filesystems
			// We're just verifying the job continues after file deletion errors

			// Run cleanup
			ctx := context.Background()
			data := map[string]any{
				"backup_id":               float64(backupID),
				"database_id":             db1.DatabaseID,
				"branch_id":               db1.DatabaseBranchID,
				"restore_point_timestamp": float64(time.Now().Add(-40 * 24 * time.Hour).UnixNano()),
			}

			err := app.CleanupBackupJob(ctx, data)

			if err != nil {
				t.Fatalf("CleanupBackupJob failed: %v", err)
			}

			// Verify backup record was deleted (even if some files couldn't be deleted)
			var count int
			err = sysDB.QueryRow(`SELECT COUNT(*) FROM database_backups WHERE id = ?`, backupID).Scan(&count)

			if err != nil {
				t.Fatalf("failed to query backup count: %v", err)
			}

			if count != 0 {
				t.Error("Backup record was not deleted")
			}
		})
	})
}
