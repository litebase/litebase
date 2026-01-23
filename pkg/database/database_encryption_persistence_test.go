package database_test

import (
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/storage"
)

func TestEncryptedDatabasePersistence(t *testing.T) {
	test.RunWithObjectStorageWithoutApp(t, func() {
		// Start first server
		s1 := test.NewTestServer(t)
		app1 := s1.App

		<-s1.Started

		// Create encrypted database and write test data
		testDB := test.MockEncryptedDatabase(app1)

		conn, err := app1.DatabaseManager.ConnectionManager().Get(testDB.DatabaseID, testDB.DatabaseBranchID)

		if err != nil {
			t.Fatalf("Failed to get connection: %v", err)
		}

		_, err = conn.GetConnection().Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)", nil)

		if err != nil {
			t.Fatalf("Failed to create table: %v", err)
		}

		_, err = conn.GetConnection().Exec("INSERT INTO test (id, value) VALUES (1, 'encrypted data')", nil)

		if err != nil {
			t.Fatalf("Failed to insert data: %v", err)
		}

		err = conn.GetConnection().Checkpoint()

		if err != nil {
			t.Fatalf("Failed to checkpoint: %v", err)
		}

		app1.DatabaseManager.ConnectionManager().Release(conn)

		t.Logf("Database ID: %s, Branch ID: %s", testDB.DatabaseID, testDB.DatabaseBranchID)

		// Check if TieredFS has dirty files before shutdown
		tieredDriver, ok := app1.Cluster.TieredFS().Driver().(*storage.TieredFileSystemDriver)

		if ok {
			t.Logf("Before shutdown - TieredFS has dirty logs: %v", tieredDriver.HasDirtyLogs())
			t.Logf("Before shutdown - TieredFS file count: %d", tieredDriver.FileCount())
		}

		t.Log("Shutting down first server...")

		// Manually flush before shutdown to see if it works
		if tieredDriver != nil {
			t.Log("Manually flushing TieredFS...")

			err := tieredDriver.Flush()

			if err != nil {
				t.Fatalf("Failed to flush TieredFS: %v", err)
			}

			t.Logf("After manual flush - TieredFS has dirty logs: %v", tieredDriver.HasDirtyLogs())
		}

		// Shutdown first server (like in real world)
		s1.Shutdown()

		t.Log("First server shutdown complete")

		// Small delay to ensure shutdown is complete
		time.Sleep(200 * time.Millisecond)

		t.Log("Starting second server...")

		// Start second server (simulating restart)
		s2 := test.NewTestServer(t)
		app2 := s2.App

		<-s2.Started

		// Try to query the encrypted database
		newDB, err := app2.DatabaseManager.GetByName(testDB.DatabaseName)

		if err != nil {
			t.Fatalf("Failed to get database after restart: %v", err)
		}

		t.Logf("Found database after restart: %s", newDB.DatabaseID)

		newBranch, err := newDB.PrimaryBranch()

		if err != nil {
			t.Fatalf("Failed to get primary branch after restart: %v", err)
		}

		t.Logf("Found primary branch after restart: %s", newBranch.DatabaseBranchID)
		t.Logf("Attempting to get connection for database: %s, branch: %s", newDB.DatabaseID, newBranch.DatabaseBranchID)

		newConn, err := app2.DatabaseManager.ConnectionManager().Get(newDB.DatabaseID, newBranch.DatabaseBranchID)

		if err != nil {
			t.Fatalf("Failed to get connection after restart: %v", err)
		}

		defer app2.DatabaseManager.ConnectionManager().Release(newConn)

		// This is the critical test - can we read the encrypted data after restart?
		result, err := newConn.GetConnection().Exec("SELECT value FROM test WHERE id = 1", nil)

		if err != nil {
			t.Fatalf("Failed to read data after restart: %v", err)
		}

		if len(result.Rows) != 1 {
			t.Fatalf("Expected 1 row, got %d", len(result.Rows))
		}

		value := string(result.Rows[0][0].Text())

		if value != "encrypted data" {
			t.Fatalf("Expected 'encrypted data', got '%s'", value)
		}

		t.Log("✓ Encrypted database persisted correctly after restart")
	})
}
