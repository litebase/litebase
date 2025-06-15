package http_test

import (
	"fmt"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/database"
	"github.com/litebase/litebase/server/storage"
)

func TestDatabaseRestoreController(t *testing.T) {
	test.Run(t, func() {
		// Force immediate compaction for testing
		originalInterval := storage.PageLoggerCompactInterval
		storage.PageLoggerCompactInterval = 0
		defer func() {
			storage.PageLoggerCompactInterval = originalInterval
		}()

		server := test.NewTestServer(t)
		defer server.Shutdown()

		source := test.MockDatabase(server.App)
		target := test.MockDatabase(server.App)

		snapshotLogger := server.App.DatabaseManager.Resources(source.DatabaseId, source.BranchId).SnapshotLogger()

		sourceDb, err := server.App.DatabaseManager.ConnectionManager().Get(source.DatabaseId, source.BranchId)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		// Create an initial checkpoint before creating the table (this will be restore point 0)
		err = sourceDb.GetConnection().Checkpoint()

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		// Create a test table and insert some data
		_, err = sourceDb.GetConnection().Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)", nil)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		err = sourceDb.GetConnection().Checkpoint()

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		// Insert a row
		err = sourceDb.GetConnection().Transaction(false, func(db *database.DatabaseConnection) error {
			_, err = db.Exec("INSERT INTO test (value) VALUES ('John Doe')", nil)

			return err
		})

		if err != nil {
			t.Fatalf("failed to insert row: %v", err)
		}

		err = sourceDb.GetConnection().Checkpoint()

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		// Release the source database connection before making the HTTP request
		server.App.DatabaseManager.ConnectionManager().Release(source.DatabaseId, source.BranchId, sourceDb)

		// Get the snapshots
		snapshotLogger.GetSnapshots()

		// Get the latest snapshot timestamp
		snapshotKeys := snapshotLogger.Keys()

		snapshot, err := snapshotLogger.GetSnapshot(snapshotKeys[len(snapshotKeys)-1])

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if len(snapshot.RestorePoints.Data) == 0 {
			t.Fatalf("Expected at least one restore point, got %d", len(snapshot.RestorePoints.Data))
		}

		// Use the second restore point (table created but no data) for deterministic behavior
		// This avoids potential issues with restoring to a completely empty database state
		if len(snapshot.RestorePoints.Data) < 2 {
			t.Fatalf("Expected at least 2 restore points, got %d", len(snapshot.RestorePoints.Data))
		}

		restorePointTimestamp := snapshot.RestorePoints.Data[1] // Table exists but no data

		restorePoint, err := snapshot.GetRestorePoint(restorePointTimestamp)

		if err != nil {
			t.Fatalf("Expected no error getting restore point for timestamp %d, got %v", restorePointTimestamp, err)
		}

		// Get a new connection for the HTTP request verification
		client := server.WithAccessKeyClient([]auth.AccessKeyStatement{
			{
				Effect:   "Allow",
				Resource: "*",
				Actions:  []auth.Privilege{auth.DatabasePrivilegeRestore},
			},
		})

		// Test the health check endpoint
		resp, responseCode, err := client.SendToDatabase(source, "restore", "POST", map[string]any{
			"target_database_id":        target.DatabaseId,
			"target_database_branch_id": target.BranchId,
			"timestamp":                 restorePoint.Timestamp,
		})

		if err != nil {
			t.Fatalf("Failed to make request: %v", err)
		}

		if responseCode != 200 {
			t.Fatalf("Expected status code 200, got %d", responseCode)
		}

		if resp["status"] != "success" {
			t.Errorf("Expected success status, got %v", resp["status"])
		}

		// Ensure the target database has the restored data
		targetDB, err := server.App.DatabaseManager.ConnectionManager().Get(target.DatabaseId, target.BranchId)

		if err != nil {
			t.Fatalf("failed to get target database connection: %v", err)
		}

		defer server.App.DatabaseManager.ConnectionManager().Release(target.DatabaseId, target.BranchId, targetDB)

		// Verify the data is restored correctly - should have the table but no data (restore point 1)
		err = targetDB.GetConnection().Transaction(true, func(db *database.DatabaseConnection) error {
			result, err := db.Exec("SELECT COUNT(*) FROM test", nil)

			if err != nil {
				return fmt.Errorf("Expected no error, got %v", err)
			}

			if result == nil || len(result.Rows) == 0 {
				return fmt.Errorf("Expected result to have rows")
			}

			count := result.Rows[0][0].Int64()
			if count != 0 {
				return fmt.Errorf("Expected 0 rows in restored table, got %d", count)
			}

			return nil
		})

		if err != nil {
			t.Fatalf("Transaction failed: %v", err)
		}
	})
}
