package http_test

import (
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/database"
	"github.com/litebase/litebase/pkg/storage"
)

func TestDatabaseRestoreController(t *testing.T) {
	test.Run(t, func() {
		// Force immediate compaction for testing
		originalInterval := storage.GetPageLoggerCompactInterval()
		storage.SetPageLoggerCompactInterval(0)
		defer func() {
			storage.SetPageLoggerCompactInterval(originalInterval)
		}()

		server := test.NewTestServer(t)
		defer server.Shutdown()

		source := test.MockDatabase(server.App)
		target := test.MockDatabase(server.App)

		snapshotLogger := server.App.DatabaseManager.Resources(source.DatabaseID, source.DatabaseBranchID).SnapshotLogger()

		sourceDb, err := server.App.DatabaseManager.ConnectionManager().Get(source.DatabaseID, source.DatabaseBranchID)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		defer server.App.DatabaseManager.ConnectionManager().Release(sourceDb)

		// Create an initial checkpoint before creating the table (this will be restore point 0)
		err = server.App.DatabaseManager.ConnectionManager().ForceCheckpoint(source.DatabaseID, source.DatabaseBranchID)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		// Create a test table and insert some data
		_, err = sourceDb.GetConnection().Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)", nil)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		err = server.App.DatabaseManager.ConnectionManager().ForceCheckpoint(source.DatabaseID, source.DatabaseBranchID)

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

		err = server.App.DatabaseManager.ConnectionManager().ForceCheckpoint(source.DatabaseID, source.DatabaseBranchID)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		// Get the snapshots
		if _, err := snapshotLogger.GetSnapshots(); err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

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
		client := server.WithAccessKeyClient([]auth.Statement{
			{
				Effect:   auth.StatementEffectAllow,
				Resource: "*",
				Actions:  []auth.Privilege{auth.DatabasePrivilegeRestore},
			},
		})

		resp, responseCode, err := client.Send(
			fmt.Sprintf(
				"/v1/databases/%s/branches/%s/restore",
				source.DatabaseName,
				source.BranchName,
			),
			"POST",
			map[string]any{
				"targetDatabase":       target.DatabaseName,
				"targetDatabaseBranch": target.BranchName,
				"timestamp":            strconv.FormatInt(restorePoint.Timestamp, 10),
			},
		)

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
		targetDB, err := server.App.DatabaseManager.ConnectionManager().Get(target.DatabaseID, target.DatabaseBranchID)

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

func TestDatabaseRestoreControllerMultiple(t *testing.T) {
	test.Run(t, func() {
		// Force immediate compaction for testing
		originalInterval := storage.GetPageLoggerCompactInterval()
		storage.SetPageLoggerCompactInterval(0)
		defer func() {
			storage.SetPageLoggerCompactInterval(originalInterval)
		}()

		server := test.NewTestServer(t)
		defer server.Shutdown()

		source := test.MockDatabase(server.App)

		snapshotLogger := server.App.DatabaseManager.Resources(source.DatabaseID, source.DatabaseBranchID).SnapshotLogger()

		sourceDb, err := server.App.DatabaseManager.ConnectionManager().Get(source.DatabaseID, source.DatabaseBranchID)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		defer server.App.DatabaseManager.ConnectionManager().Release(sourceDb)

		// Create an initial checkpoint before creating the table (this will be restore point 0)
		err = server.App.DatabaseManager.ConnectionManager().ForceCheckpoint(source.DatabaseID, source.DatabaseBranchID)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		_, err = sourceDb.GetConnection().Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)", nil)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		err = server.App.DatabaseManager.ConnectionManager().ForceCheckpoint(source.DatabaseID, source.DatabaseBranchID)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		// Insert rows
		for i := range 10 {
			err = sourceDb.GetConnection().Transaction(false, func(db *database.DatabaseConnection) error {
				_, err = db.Exec("INSERT INTO test (value) VALUES ('John Doe')", nil)

				return err
			})

			if err != nil {
				t.Fatalf("failed to insert row: %v", err)
			}

			err = server.App.DatabaseManager.ConnectionManager().ForceCheckpoint(source.DatabaseID, source.DatabaseBranchID)

			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}

			// Ensure snapshots are updated after each checkpoint
			snapshots, err := snapshotLogger.GetSnapshotsWithRestorePoints()

			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}

			for _, snapshot := range snapshots {
				if snapshot.RestorePoints.Total != i+3 {
					t.Fatalf("Expected %d restore points, got %d for iteration %d", i+3, snapshot.RestorePoints.Total, i)
				}
				break
			}
		}

		// Verify the source database has 10 rows
		var sourceRowCount int64
		err = sourceDb.GetConnection().Transaction(true, func(db *database.DatabaseConnection) error {
			result, err := db.Exec("SELECT COUNT(*) FROM test", nil)

			if err != nil {
				return err
			}

			sourceRowCount = result.Rows[0][0].Int64()

			return nil
		})

		if err != nil {
			t.Fatalf("failed to count rows in source database: %v", err)
		}

		if sourceRowCount != 10 {
			t.Fatalf("Expected source database to have 10 rows, got %d", sourceRowCount)
		}

		// Get the snapshots once before the loop to ensure consistency
		snapshots, err := snapshotLogger.GetSnapshotsWithRestorePoints()

		for _, snapshot := range snapshots {
			if snapshot.RestorePoints.Total != 12 {
				t.Fatalf("Expected 12 restore points, got %d", snapshot.RestorePoints.Total)
			}

			break
		}

		// Verify that the source database actually has 10 rows before starting restore tests
		err = sourceDb.GetConnection().Transaction(true, func(db *database.DatabaseConnection) error {
			result, err := db.Exec("SELECT COUNT(*) FROM test", nil)
			if err != nil {
				return err
			}

			count := result.Rows[0][0].Int64()

			if count != 10 {
				return fmt.Errorf("Expected source database to have 10 rows, got %d", count)
			}

			return nil
		})

		if err != nil {
			t.Fatalf("Source database verification failed: %v", err)
		}

		// Get the latest snapshot timestamp
		snapshotKeys := snapshotLogger.Keys()

		snapshot, err := snapshotLogger.GetSnapshot(snapshotKeys[len(snapshotKeys)-1])

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if len(snapshot.RestorePoints.Data) == 0 {
			t.Fatalf("Expected at least one restore point, got %d", len(snapshot.RestorePoints.Data))
		}

		if len(snapshot.RestorePoints.Data) < 12 {
			t.Fatalf("Expected at least 12 restore points, got %d", len(snapshot.RestorePoints.Data))
		}

		// Get a single client for all HTTP requests
		client := server.WithAccessKeyClient([]auth.Statement{
			{
				Effect:   auth.StatementEffectAllow,
				Resource: "*",
				Actions:  []auth.Privilege{auth.DatabasePrivilegeRestore},
			},
		})

		// Get the current count of the rows in the database
		var currentRowCount int64
		err = sourceDb.GetConnection().Transaction(true, func(db *database.DatabaseConnection) error {
			result, err := db.Exec("SELECT COUNT(*) FROM test", nil)

			if err != nil {
				return err
			}

			currentRowCount = result.Rows[0][0].Int64()

			return nil
		})

		if err != nil {
			t.Fatalf("failed to count rows in source database: %v", err)
		}

		if currentRowCount != 10 {
			t.Fatalf("Expected source database to have 10 rows, got %d", currentRowCount)
		}

		for i := 10; i > 0; i-- {
			restorePointTimestamp := snapshot.RestorePoints.Data[i+1] // i+1 because we have 2 initial restore points (0: empty, 1: table created)

			restorePoint, err := snapshot.GetRestorePoint(restorePointTimestamp)

			if err != nil {
				t.Fatalf("Expected no error getting restore point for timestamp %d, got %v", restorePointTimestamp, err)
			}

			target := test.MockDatabase(server.App)

			resp, responseCode, err := client.Send(
				fmt.Sprintf(
					"/v1/databases/%s/branches/%s/restore",
					source.DatabaseName,
					source.BranchName,
				),
				"POST",
				map[string]any{
					"targetDatabase":       target.DatabaseName,
					"targetDatabaseBranch": target.BranchName,
					"timestamp":            strconv.FormatInt(restorePoint.Timestamp, 10),
				},
			)

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
			targetDB, err := server.App.DatabaseManager.ConnectionManager().Get(target.DatabaseID, target.DatabaseBranchID)

			if err != nil {
				t.Fatalf("failed to get target database connection: %v", err)
			}

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

				if count != int64(i-1) {
					return fmt.Errorf("Expected %d rows in restored table, got %d (restore point index %d, timestamp %d)", i-1, count, i+1, restorePointTimestamp)
				}

				return nil
			})

			// Release the connection immediately after use
			server.App.DatabaseManager.ConnectionManager().Release(targetDB)

			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}
		}
	})
}

func TestDatabaseRestoreControllerNonEmptyTarget(t *testing.T) {
	test.Run(t, func() {
		// Force immediate compaction for testing
		originalInterval := storage.GetPageLoggerCompactInterval()
		storage.SetPageLoggerCompactInterval(0)
		defer func() {
			storage.SetPageLoggerCompactInterval(originalInterval)
		}()

		server := test.NewTestServer(t)
		defer server.Shutdown()

		source := test.MockDatabase(server.App)
		target := test.MockDatabase(server.App)

		// Set up source database with data
		sourceDb, err := server.App.DatabaseManager.ConnectionManager().Get(source.DatabaseID, source.DatabaseBranchID)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		defer server.App.DatabaseManager.ConnectionManager().Release(sourceDb)

		// Create initial checkpoint
		err = server.App.DatabaseManager.ConnectionManager().ForceCheckpoint(source.DatabaseID, source.DatabaseBranchID)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		// Create table and data in source
		_, err = sourceDb.GetConnection().Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)", nil)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		// Force checkpoint to create restore point
		err = server.App.DatabaseManager.ConnectionManager().ForceCheckpoint(source.DatabaseID, source.DatabaseBranchID)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		// Insert data in source
		err = sourceDb.GetConnection().Transaction(false, func(db *database.DatabaseConnection) error {
			_, err = db.Exec("INSERT INTO test (value) VALUES ('source data')", nil)
			return err
		})

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		// Final checkpoint to establish restore point
		err = server.App.DatabaseManager.ConnectionManager().ForceCheckpoint(source.DatabaseID, source.DatabaseBranchID)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		// Set up target database with existing data (make it non-empty)
		targetDb, err := server.App.DatabaseManager.ConnectionManager().Get(target.DatabaseID, target.DatabaseBranchID)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		defer server.App.DatabaseManager.ConnectionManager().Release(targetDb)

		// Add data to target database to make it non-empty
		_, err = targetDb.GetConnection().Exec("CREATE TABLE existing (id INTEGER PRIMARY KEY, data TEXT)", nil)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		err = targetDb.GetConnection().Transaction(false, func(db *database.DatabaseConnection) error {
			_, err = db.Exec("INSERT INTO existing (data) VALUES ('existing data')", nil)
			return err
		})

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		// Force checkpoint on target to ensure data is persisted
		err = server.App.DatabaseManager.ConnectionManager().ForceCheckpoint(target.DatabaseID, target.DatabaseBranchID)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		// Get restore point from source
		snapshotLogger := server.App.DatabaseManager.Resources(source.DatabaseID, source.DatabaseBranchID).SnapshotLogger()
		snapshotKeys := snapshotLogger.Keys()

		if len(snapshotKeys) == 0 {
			t.Fatal("No snapshots found")
		}

		snapshot, err := snapshotLogger.GetSnapshot(snapshotKeys[len(snapshotKeys)-1])

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if len(snapshot.RestorePoints.Data) == 0 {
			t.Fatal("No restore points found")
		}

		// Use the last restore point
		restorePointTimestamp := snapshot.RestorePoints.End
		restorePoint, err := snapshot.GetRestorePoint(restorePointTimestamp)

		if err != nil {
			t.Fatalf("Expected no error getting restore point for timestamp %d, got %v", restorePointTimestamp, err)
		}

		// Attempt to restore to non-empty target - should fail
		client := server.WithAccessKeyClient([]auth.Statement{
			{
				Effect:   auth.StatementEffectAllow,
				Resource: "*",
				Actions:  []auth.Privilege{auth.DatabasePrivilegeRestore},
			},
		})

		resp, responseCode, err := client.Send(
			fmt.Sprintf(
				"/v1/databases/%s/branches/%s/restore",
				source.DatabaseName,
				source.BranchName,
			),
			"POST",
			map[string]any{
				"targetDatabase":       target.DatabaseName,
				"targetDatabaseBranch": target.BranchName,
				"timestamp":            strconv.FormatInt(restorePoint.Timestamp, 10),
			},
		)

		if err != nil {
			t.Fatalf("Failed to make request: %v", err)
		}

		// Should return 400 Bad Request due to non-empty target
		if responseCode != 400 {
			t.Log("Response:", resp)
			t.Fatalf("Expected status code 400, got %d", responseCode)
		}

		// Verify the error message mentions non-empty database
		if message, ok := resp["message"].(string); ok {
			if !strings.Contains(message, "not empty") {
				t.Errorf("Expected error message to mention 'not empty', got: %s", message)
			}
		} else {
			t.Error("Expected error message in response")
		}
	})
}
