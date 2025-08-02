package http_test

import (
	"fmt"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/backups"
	"github.com/litebase/litebase/pkg/database"
)

func TestDatabaseSnapshotIndexController(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		mock := test.MockDatabase(server.App)

		db, err := server.App.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		defer server.App.DatabaseManager.ConnectionManager().Release(db)

		// Create an initial checkpoint before creating the table (this will be restore point 0)
		db.GetConnection().Checkpoint()

		// Create a test table and insert some data
		_, err = db.GetConnection().Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)", nil)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		db.GetConnection().Checkpoint()

		// Insert a row
		err = db.GetConnection().Transaction(false, func(db *database.DatabaseConnection) error {
			_, err = db.Exec("INSERT INTO test (value) VALUES ('John Doe')", nil)

			return err
		})

		if err != nil {
			t.Fatalf("failed to insert row: %v", err)
		}

		db.GetConnection().Checkpoint()

		// Get a new connection for the HTTP request verification
		client := server.WithAccessKeyClient([]auth.AccessKeyStatement{
			{
				Effect:   "Allow",
				Resource: "*",
				Actions:  []auth.Privilege{auth.DatabasePrivilegeBackup},
			},
		})

		resp, responseCode, err := client.Send(
			fmt.Sprintf(
				"/v1/databases/%s/%s/snapshots",
				mock.DatabaseName,
				mock.BranchName,
			),
			"GET",
			nil,
		)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if responseCode != 200 {
			t.Fatalf("Expected response code 200, got %d: %s", responseCode, resp)
		}

		// There should be 1 snapshot
		if len(resp["data"].([]any)) != 1 {
			t.Fatalf("Expected 1 snapshot, got %d", len(resp["data"].([]any)))
		}
	})
}

func TestDatabaseSnapshotShowController(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()
		mock := test.MockDatabase(server.App)

		snapshotLogger := server.App.DatabaseManager.Resources(mock.DatabaseID, mock.DatabaseBranchID).SnapshotLogger()

		db, err := server.App.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		defer server.App.DatabaseManager.ConnectionManager().Release(db)

		// Create an initial checkpoint before creating the table (this will be restore point 0)
		db.GetConnection().Checkpoint()

		// Create a test table and insert some data
		_, err = db.GetConnection().Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)", nil)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		db.GetConnection().Checkpoint()

		// Insert a row
		err = db.GetConnection().Transaction(false, func(db *database.DatabaseConnection) error {
			_, err = db.Exec("INSERT INTO test (value) VALUES ('John Doe')", nil)

			return err
		})

		if err != nil {
			t.Fatalf("failed to insert row: %v", err)
		}

		db.GetConnection().Checkpoint()

		snapshots, err := snapshotLogger.GetSnapshots()

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if len(snapshots) != 1 {
			t.Fatalf("Expected 1 snapshot, got %d", len(snapshots))
		}

		// Get a new connection for the HTTP request verification
		client := server.WithAccessKeyClient([]auth.AccessKeyStatement{
			{
				Effect:   "Allow",
				Resource: "*",
				Actions:  []auth.Privilege{auth.DatabasePrivilegeBackup},
			},
		})

		var snapshot *backups.Snapshot

		for _, s := range snapshots {
			snapshot = s
		}

		resp, responseCode, err := client.Send(
			fmt.Sprintf(
				"/v1/databases/%s/%s/snapshots/%d",
				mock.DatabaseName,
				mock.BranchName,
				snapshot.Timestamp,
			),
			"GET",
			nil,
		)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if responseCode != 200 {
			t.Fatalf("Expected response code 200, got %d: %s", responseCode, resp)
		}

		// There should be 1 snapshot
		if resp["data"].(map[string]any)["timestamp"] == 0 {
			t.Fatal("Expected snapshot timestamp to be set, got 0")
		}

		// The snapshot timestamp should match
		if int64(resp["data"].(map[string]any)["timestamp"].(float64)) != snapshot.Timestamp {
			t.Fatalf("Expected snapshot timestamp to be %d, got %d", snapshot.Timestamp, resp["data"].(map[string]any)["timestamp"])
		}

		// The snapshot should have 3 restore points
		if len(resp["data"].(map[string]any)["restore_points"].(map[string]any)["data"].([]any)) != 3 {
			t.Fatalf("Expected snapshot to have 3 restore points, got %d", len(resp["data"].(map[string]any)["restore_points"].(map[string]any)["data"].([]any)))
		}
	})
}
