package backups_test

import (
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/backups"
	"github.com/litebase/litebase/pkg/server"
)

func TestSnapshotLoggerEncryption(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		// Create an encrypted database using test helper
		db := test.MockEncryptedDatabase(app)
		branch := db.Branch

		// Get resources - these should be created with encryption already configured
		resources := app.DatabaseManager.Resources(branch)

		if resources == nil {
			t.Fatalf("Failed to get resources")
		}

		// Get snapshot logger
		snapshotLogger := resources.SnapshotLogger()

		if snapshotLogger == nil {
			t.Fatalf("Failed to get snapshot logger")
		}

		// Create a snapshot
		timestamp := time.Now().UTC().UnixNano()
		snapshot, err := snapshotLogger.GetSnapshot(timestamp)

		if err != nil {
			t.Fatalf("Failed to get snapshot: %v", err)
		}

		// Log a restore point
		err = snapshot.Log(timestamp, 10)

		if err != nil {
			t.Fatalf("Failed to log restore point: %v", err)
		}

		// Close the snapshot to ensure it's written
		err = snapshot.Close()

		if err != nil {
			t.Fatalf("Failed to close snapshot: %v", err)
		}

		// Verify the snapshot file is encrypted (should start with "LSTR" magic header)
		// Use the proper GetSnapshotPath function
		snapshotPath := backups.GetSnapshotPath(db.DatabaseID, branch.DatabaseBranchID, snapshot.Timestamp)

		// Read the file from tiered FS
		data, err := app.Cluster.TieredFS().ReadFile(snapshotPath)

		if err != nil {
			t.Fatalf("Failed to read snapshot file: %v", err)
		}

		if len(data) < 4 {
			t.Fatalf("Snapshot file is too small: %d bytes", len(data))
		}

		magicHeader := string(data[:4])

		if magicHeader != "LSTR" {
			t.Errorf("Expected snapshot file to have LSTR magic header, got: %s (hex: %x)",
				magicHeader, data[:4])
		} else {
			t.Logf("✓ Snapshot file is encrypted with LSTR header")
		}
	})
}

func TestRollbackLoggerEncryption(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		// Create an encrypted database using test helper
		db := test.MockEncryptedDatabase(app)
		branch := db.Branch

		// Get resources - these should be created with encryption already configured
		resources := app.DatabaseManager.Resources(branch)

		if resources == nil {
			t.Fatalf("Failed to get resources")
		}

		// Get rollback logger
		rollbackLogger := resources.RollbackLogger()

		if rollbackLogger == nil {
			t.Fatalf("Failed to get rollback logger")
		}

		// Create a rollback log entry
		timestamp := time.Now().UTC().UnixNano()
		rollbackLog, err := rollbackLogger.GetLog(timestamp)

		if err != nil {
			t.Fatalf("Failed to get rollback log: %v", err)
		}

		// Create an AppendFrame first (required)
		offset, size, err := rollbackLog.AppendFrame(timestamp)

		if err != nil {
			t.Fatalf("Failed to append frame: %v", err)
		}

		if offset < 0 || size <= 0 {
			t.Fatalf("Invalid frame offset/size: %d/%d", offset, size)
		}

		// Close the rollback log to ensure it's written
		err = rollbackLog.Close()

		if err != nil {
			t.Fatalf("Failed to close rollback log: %v", err)
		}

		// Verify the rollback log file is encrypted (should start with "LSTR" magic header)
		// Get the timestamp of the log (truncated to hour by rollback logger)
		rollbackPath := backups.GetRollbackLogPath(db.DatabaseID, branch.DatabaseBranchID, rollbackLog.Timestamp)

		// Read the file from tiered FS
		data, err := app.Cluster.TieredFS().ReadFile(rollbackPath)

		if err != nil {
			t.Fatalf("Failed to read rollback file: %v", err)
		}

		if len(data) < 4 {
			t.Fatalf("Rollback file is too small: %d bytes", len(data))
		}

		magicHeader := string(data[:4])

		if magicHeader != "LSTR" {
			t.Errorf("Expected rollback file to have LSTR magic header, got: %s (hex: %x)",
				magicHeader, data[:4])
		} else {
			t.Logf("✓ Rollback log file is encrypted with LSTR header")
		}
	})
}
