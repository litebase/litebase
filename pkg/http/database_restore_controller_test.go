package http_test

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/database"
	"github.com/litebase/litebase/pkg/storage"
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

		snapshotLogger := server.App.DatabaseManager.Resources(source.DatabaseID, source.BranchID).SnapshotLogger()

		sourceDb, err := server.App.DatabaseManager.ConnectionManager().Get(source.DatabaseID, source.BranchID)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		defer server.App.DatabaseManager.ConnectionManager().Release(sourceDb)

		// Create an initial checkpoint before creating the table (this will be restore point 0)
		err = server.App.DatabaseManager.ConnectionManager().ForceCheckpoint(source.DatabaseID, source.BranchID)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		// Create a test table and insert some data
		_, err = sourceDb.GetConnection().Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)", nil)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		err = server.App.DatabaseManager.ConnectionManager().ForceCheckpoint(source.DatabaseID, source.BranchID)

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

		err = server.App.DatabaseManager.ConnectionManager().ForceCheckpoint(source.DatabaseID, source.BranchID)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

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

		// Use the last restore point for deterministic behavior
		if len(snapshot.RestorePoints.Data) < 2 {
			t.Fatalf("Expected at least 2 restore points, got %d", len(snapshot.RestorePoints.Data))
		}

		restorePointTimestamp := snapshot.RestorePoints.End // Table exists but no data

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

		resp, responseCode, err := client.Send(fmt.Sprintf("/%s/restore", source.DatabaseKey.Key), "POST", map[string]any{
			"target_database_id":        target.DatabaseID,
			"target_database_branch_id": target.BranchID,
			"timestamp":                 strconv.FormatInt(restorePoint.Timestamp, 10),
		})

		if err != nil {
			t.Fatalf("Failed to make request: %v", err)
		}

		if responseCode != 200 {
			t.Log("Response:", resp)
			t.Fatalf("Expected status code 200, got %d", responseCode)
		}

		if resp["status"] != "success" {
			t.Errorf("Expected success status, got %v", resp["status"])
		}

		// Ensure the target database has the restored data
		targetDB, err := server.App.DatabaseManager.ConnectionManager().Get(target.DatabaseID, target.BranchID)

		if err != nil {
			t.Fatalf("failed to get target database connection: %v", err)
		}

		defer server.App.DatabaseManager.ConnectionManager().Release(targetDB)

		// Verify the data is restored correctly - should have the table but no data (restore point 1)
		err = targetDB.GetConnection().Transaction(true, func(db *database.DatabaseConnection) error {
			result, err := db.Exec("SELECT COUNT(*) FROM test", nil)

			if err != nil {
				return fmt.Errorf("Expected no error, got %v", err)
			}

			if len(result.Rows) != 1 {
				return fmt.Errorf("Expected result to have one row, got %v", len(result.Rows))
			}

			count := result.Rows[0][0].Int64()
			if count != 1 {
				return fmt.Errorf("Expected 1 row in restored table, got %d", count)
			}

			return nil
		})

		if err != nil {
			t.Fatalf("Transaction failed: %v", err)
		}
	})
}

func TestDatabaseRestoreControllerMultiple(t *testing.T) {
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

		snapshotLogger := server.App.DatabaseManager.Resources(source.DatabaseID, source.BranchID).SnapshotLogger()

		sourceDb, err := server.App.DatabaseManager.ConnectionManager().Get(source.DatabaseID, source.BranchID)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		defer server.App.DatabaseManager.ConnectionManager().Release(sourceDb)

		// Create an initial checkpoint before creating the table (this will be restore point 0)
		err = server.App.DatabaseManager.ConnectionManager().ForceCheckpoint(source.DatabaseID, source.BranchID)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		// Create a test table and insert some data
		_, err = sourceDb.GetConnection().Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)", nil)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		err = server.App.DatabaseManager.ConnectionManager().ForceCheckpoint(source.DatabaseID, source.BranchID)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		// Insert rows
		for range 10 {
			err = sourceDb.GetConnection().Transaction(false, func(db *database.DatabaseConnection) error {
				_, err = db.Exec("INSERT INTO test (value) VALUES ('John Doe')", nil)

				return err
			})

			if err != nil {
				t.Fatalf("failed to insert row: %v", err)
			}

			err = server.App.DatabaseManager.ConnectionManager().ForceCheckpoint(source.DatabaseID, source.BranchID)

			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}
		}

		for i := 10; i > 0; i-- {
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

			if len(snapshot.RestorePoints.Data) < i+2 {
				t.Fatalf("Expected at least %d restore points, got %d", i+2, len(snapshot.RestorePoints.Data))
			}

			restorePointTimestamp := snapshot.RestorePoints.Data[i+1]

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

			target := test.MockDatabase(server.App)

			resp, responseCode, err := client.Send(fmt.Sprintf("/%s/restore", source.DatabaseKey.Key), "POST", map[string]any{
				"target_database_id":        target.DatabaseID,
				"target_database_branch_id": target.BranchID,
				"timestamp":                 strconv.FormatInt(restorePoint.Timestamp, 10),
			})

			if err != nil {
				t.Fatalf("Failed to make request: %v", err)
			}

			if responseCode != 200 {
				t.Log("Response:", resp)
				t.Fatalf("Expected status code 200, got %d", responseCode)
			}

			if resp["status"] != "success" {
				t.Errorf("Expected success status, got %v", resp["status"])
			}

			// Ensure the target database has the restored data
			targetDB, err := server.App.DatabaseManager.ConnectionManager().Get(target.DatabaseID, target.BranchID)

			if err != nil {
				t.Fatalf("failed to get target database connection: %v", err)
			}

			defer server.App.DatabaseManager.ConnectionManager().Release(targetDB)

			// Verify the data is restored correctly - should have the table but no data (restore point 1)
			err = targetDB.GetConnection().Transaction(true, func(db *database.DatabaseConnection) error {
				result, err := db.Exec("SELECT COUNT(*) FROM test", nil)

				if err != nil {
					return fmt.Errorf("Expected no error, got %v", err)
				}

				if len(result.Rows) != 1 {
					return fmt.Errorf("Expected result to have one row, got %v", len(result.Rows))
				}

				count := result.Rows[0][0].Int64()
				if count != int64(i) {
					return fmt.Errorf("Expected %d rows in restored table, got %d", i, count)
				}

				return nil
			})

			if err != nil {
				t.Fatalf("Transaction failed: %v", err)
			}
		}
	})
}
