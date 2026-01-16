package server_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/server"
)

func TestBackupJob_NextAtCalculation(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		db, err := app.DatabaseManager.Get(mock.DatabaseID)

		if err != nil {
			t.Fatalf("failed to get database: %v", err)
		}

		branch, err := db.PrimaryBranch()

		if err != nil {
			t.Fatalf("failed to get primary branch: %v", err)
		}

		// Create some database content so backups can succeed
		conn, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

		if err != nil {
			t.Fatalf("failed to get connection: %v", err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(conn)

		_, err = conn.GetConnection().Exec("CREATE TABLE test_backup (id INTEGER PRIMARY KEY, data TEXT)", nil)

		if err != nil {
			t.Fatalf("failed to create test table: %v", err)
		}

		_, err = conn.GetConnection().Exec("INSERT INTO test_backup (data) VALUES ('test data')", nil)

		if err != nil {
			t.Fatalf("failed to insert test data: %v", err)
		}

		if err := conn.Checkpoint(); err != nil {
			t.Fatalf("failed to checkpoint: %v", err)
		}

		sysDB, err := app.DatabaseManager.SystemDatabase().DB()

		if err != nil {
			t.Fatalf("failed to get system db: %v", err)
		}

		// Helper to get current backup_next_at
		getNextAt := func() (int64, bool) {
			var nextAt sql.NullInt64
			err := sysDB.QueryRow(`SELECT backup_next_at FROM database_branch_settings WHERE database_branch_reference_id = ?`, branch.ID).Scan(&nextAt)

			if err != nil {
				t.Fatalf("failed to read backup_next_at: %v", err)
			}

			return nextAt.Int64, nextAt.Valid
		}

		// Helper to set backup_next_at
		setNextAt := func(timestamp int64) {
			_, err := sysDB.Exec(`UPDATE database_branch_settings SET backup_next_at = ? WHERE database_branch_reference_id = ?`, timestamp, branch.ID)

			if err != nil {
				t.Fatalf("failed to set backup_next_at: %v", err)
			}
		}

		t.Run("NormalBackup_NoTimeSkew", func(t *testing.T) {
			// Set backup scheduled for 1 hour ago
			scheduledTime := time.Now().UTC().Add(-1 * time.Hour).Unix()

			setNextAt(scheduledTime)

			// Run backup job
			ctx := context.Background()

			err := app.BackupJob(ctx, map[string]any{
				"database_id": mock.DatabaseID,
				"branch_id":   mock.DatabaseBranchID,
			})

			if err != nil {
				t.Fatalf("BackupJob failed: %v", err)
			}

			// Check next backup is scheduled 24h from the original scheduled time
			nextAt, valid := getNextAt()

			if !valid {
				t.Fatal("backup_next_at should be set")
			}

			expectedNext := scheduledTime + (24 * 3600)

			if nextAt != expectedNext {
				t.Errorf("Expected next backup at %d (24h after scheduled), got %d", expectedNext, nextAt)
			}

			// Verify no time drift - should be ~23 hours from now (since we started 1h ago + 24h interval)
			hoursUntilNext := float64(nextAt-time.Now().UTC().Unix()) / 3600.0

			if hoursUntilNext < 22.9 || hoursUntilNext > 23.1 {
				t.Errorf("Expected ~23 hours until next backup, got %.2f hours", hoursUntilNext)
			}
		})

		t.Run("LateBackup_SkipsToNextFutureSlot", func(t *testing.T) {
			// Set backup scheduled for 25 hours ago (missed one interval)
			scheduledTime := time.Now().UTC().Add(-25 * time.Hour).Unix()

			setNextAt(scheduledTime)

			// Run backup job
			ctx := context.Background()

			err := app.BackupJob(ctx, map[string]any{
				"database_id": mock.DatabaseID,
				"branch_id":   mock.DatabaseBranchID,
			})

			if err != nil {
				t.Fatalf("BackupJob failed: %v", err)
			}

			// Should skip the missed slot and schedule next future one
			nextAt, valid := getNextAt()

			if !valid {
				t.Fatal("backup_next_at should be set")
			}

			now := time.Now().UTC().Unix()

			if nextAt <= now {
				t.Errorf("Next backup should be in the future, but got %d (now is %d)", nextAt, now)
			}

			// Should be ~23 hours from now (since we're catching up from -25h to next future slot)
			hoursUntilNext := float64(nextAt-now) / 3600.0

			if hoursUntilNext < 22.9 || hoursUntilNext > 23.1 {
				t.Errorf("Expected ~23 hours until next backup, got %.2f hours", hoursUntilNext)
			}
		})

		t.Run("VeryLateBackup_MultipleIntervalsSkipped", func(t *testing.T) {
			// Set backup scheduled for 73 hours ago (3 days+1h, should skip 3 intervals)
			scheduledTime := time.Now().UTC().Add(-73 * time.Hour).Unix()
			setNextAt(scheduledTime)

			// Run backup job
			ctx := context.Background()

			err := app.BackupJob(ctx, map[string]any{
				"database_id": mock.DatabaseID,
				"branch_id":   mock.DatabaseBranchID,
			})

			if err != nil {
				t.Fatalf("BackupJob failed: %v", err)
			}

			// Should skip all missed slots and schedule next future one
			nextAt, valid := getNextAt()

			if !valid {
				t.Fatal("backup_next_at should be set")
			}

			now := time.Now().UTC().Unix()

			if nextAt <= now {
				t.Errorf("Next backup should be in the future, but got %d (now is %d)", nextAt, now)
			}

			// Should be ~23 hours from now
			hoursUntilNext := float64(nextAt-now) / 3600.0

			if hoursUntilNext < 22.9 || hoursUntilNext > 23.1 {
				t.Errorf("Expected ~23 hours until next backup, got %.2f hours", hoursUntilNext)
			}
		})

		t.Run("FirstBackup_NoScheduledTime", func(t *testing.T) {
			// Clear backup_next_at (simulating first backup)
			_, err := sysDB.Exec(`UPDATE database_branch_settings SET backup_next_at = NULL WHERE database_branch_reference_id = ?`, branch.ID)

			if err != nil {
				t.Fatalf("failed to clear backup_next_at: %v", err)
			}

			beforeRun := time.Now().UTC().Unix()

			// Run backup job
			ctx := context.Background()

			err = app.BackupJob(ctx, map[string]any{
				"database_id": mock.DatabaseID,
				"branch_id":   mock.DatabaseBranchID,
			})

			if err != nil {
				t.Fatalf("BackupJob failed: %v", err)
			}

			afterRun := time.Now().UTC().Unix()

			// Should schedule 24h from now
			nextAt, valid := getNextAt()

			if !valid {
				t.Fatal("backup_next_at should be set")
			}

			// Should be between (now + 24h - 1s) and (now + 24h + 1s)
			expectedMin := beforeRun + (24 * 3600)
			expectedMax := afterRun + (24 * 3600)

			if nextAt < expectedMin || nextAt > expectedMax {
				t.Errorf("Expected next backup between %d and %d, got %d", expectedMin, expectedMax, nextAt)
			}
		})

		t.Run("CustomInterval_48Hours", func(t *testing.T) {
			// Update interval to 48h
			settings, err := branch.GetBranchSettings()

			if err != nil {
				t.Fatalf("failed to get settings: %v", err)
			}

			settings.BackupInterval = "48h"

			err = branch.UpdateBranchSettings(settings)

			if err != nil {
				t.Fatalf("failed to update settings: %v", err)
			}

			// Set backup scheduled for 2 hours ago
			scheduledTime := time.Now().UTC().Add(-2 * time.Hour).Unix()
			setNextAt(scheduledTime)

			// Run backup job
			ctx := context.Background()

			err = app.BackupJob(ctx, map[string]any{
				"database_id": mock.DatabaseID,
				"branch_id":   mock.DatabaseBranchID,
			})

			if err != nil {
				t.Fatalf("BackupJob failed: %v", err)
			}

			// Check next backup is scheduled 48h from the original scheduled time
			nextAt, valid := getNextAt()

			if !valid {
				t.Fatal("backup_next_at should be set")
			}

			expectedNext := scheduledTime + (48 * 3600)

			if nextAt != expectedNext {
				t.Errorf("Expected next backup at %d (48h after scheduled), got %d", expectedNext, nextAt)
			}

			// Verify ~46 hours from now (since we started 2h ago + 48h interval)
			hoursUntilNext := float64(nextAt-time.Now().UTC().Unix()) / 3600.0

			if hoursUntilNext < 45.9 || hoursUntilNext > 46.1 {
				t.Errorf("Expected ~46 hours until next backup, got %.2f hours", hoursUntilNext)
			}
		})
	})
}
