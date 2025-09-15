package cmd_test

import (
	"fmt"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/backups"
	"github.com/litebase/litebase/pkg/database"
)

func TestDatabaseSnapshotShow(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		cli := test.NewTestCLI(t, server.App).
			WithServer(server).
			WithAccessKey([]auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		testDatabase := test.MockDatabase(server.App)
		snapshotLogger := server.App.DatabaseManager.Resources(testDatabase.DatabaseID, testDatabase.DatabaseBranchID).SnapshotLogger()

		db, err := server.App.DatabaseManager.ConnectionManager().Get(testDatabase.DatabaseID, testDatabase.DatabaseBranchID)

		if err != nil {
			t.Fatalf("failed to get database connection: %v", err)
		}

		defer server.App.DatabaseManager.ConnectionManager().Release(db)

		// Create an initial checkpoint to create a snapshot
		if err := db.GetConnection().Checkpoint(); err != nil {
			t.Fatalf("failed to create checkpoint: %v", err)
		}

		// Create a test table and insert some data
		_, err = db.GetConnection().Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)", nil)

		if err != nil {
			t.Fatalf("failed to create test table: %v", err)
		}

		if err := db.GetConnection().Checkpoint(); err != nil {
			t.Fatalf("failed to create checkpoint: %v", err)
		}

		// Insert a row
		err = db.GetConnection().Transaction(false, func(db *database.DatabaseConnection) error {
			_, err = db.Exec("INSERT INTO test (value) VALUES ('test data')", nil)
			return err
		})

		if err != nil {
			t.Fatalf("failed to insert data: %v", err)
		}

		if err := db.GetConnection().Checkpoint(); err != nil {
			t.Fatalf("failed to create final checkpoint: %v", err)
		}

		// Get a snapshot to show
		snapshots, err := snapshotLogger.GetSnapshots()

		if err != nil {
			t.Fatalf("failed to get snapshots: %v", err)
		}

		if len(snapshots) == 0 {
			t.Fatalf("expected at least one snapshot, got none")
		}

		// Use the first snapshot
		var snapshot *backups.Snapshot
		for _, s := range snapshots {
			snapshot = s
			break
		}

		err = cli.Run("database", "snapshot", "show", fmt.Sprintf("%s/%s", testDatabase.DatabaseName, testDatabase.BranchName), fmt.Sprintf("%d", snapshot.Timestamp))

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		// Debug: Print actual output to see what we get
		output := cli.GetOutput()
		t.Logf("Show output: %s", output)
		t.Logf("Looking for timestamp: %d", snapshot.Timestamp)

		// Check that snapshot information is displayed
		if cli.DoesNotSee("Database Snapshot") {
			t.Error("expected output to contain 'Database Snapshot' card title")
		}

		// Check for timestamp
		timestampStr := fmt.Sprintf("%d", snapshot.Timestamp)
		if cli.DoesNotSee(timestampStr) {
			t.Errorf("expected output to contain timestamp %s", timestampStr)
		}

		if cli.DoesNotSee(testDatabase.DatabaseID) {
			t.Error("expected output to contain database ID")
		}

		if cli.DoesNotSee(testDatabase.DatabaseBranchID) {
			t.Error("expected output to contain branch ID")
		}

		// Check for restore points table
		if cli.DoesNotSee("Restore Points") {
			t.Error("expected output to contain 'Restore Points' section title")
		}

		if cli.DoesNotSee("Time (UTC)") {
			t.Error("expected output to contain 'Time (UTC)' column header in restore points table")
		}
	})
}

func TestDatabaseSnapshotShowNonExistentSnapshot(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		cli := test.NewTestCLI(t, server.App).
			WithServer(server).
			WithAccessKey([]auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		testDatabase := test.MockDatabase(server.App)

		err := cli.Run("database", "snapshot", "show", fmt.Sprintf("%s/%s", testDatabase.DatabaseName, testDatabase.BranchName), "999999999")

		if err == nil {
			t.Error("expected error when showing non-existent snapshot, got none")
		}
	})
}

func TestDatabaseSnapshotShowNonExistentDatabase(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		cli := test.NewTestCLI(t, server.App).
			WithServer(server).
			WithAccessKey([]auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		err := cli.Run("database", "snapshot", "show", "non-existent-database/main", "123456789")

		if err == nil {
			t.Error("expected error when showing snapshot from non-existent database, got none")
		}
	})
}

func TestDatabaseSnapshotShowNonExistentBranch(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		cli := test.NewTestCLI(t, server.App).
			WithServer(server).
			WithAccessKey([]auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		testDatabase := test.MockDatabase(server.App)

		err := cli.Run("database", "snapshot", "show", fmt.Sprintf("%s/non-existent-branch", testDatabase.DatabaseName), "123456789")

		if err == nil {
			t.Error("expected error when showing snapshot from non-existent branch, got none")
		}
	})
}

func TestDatabaseSnapshotShowInvalidPath(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		cli := test.NewTestCLI(t, server.App).
			WithServer(server).
			WithAccessKey([]auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		err := cli.Run("database", "snapshot", "show", "invalid-path-format", "123456789")

		if err == nil {
			t.Error("expected error when using invalid path format, got none")
		}
	})
}

func TestDatabaseSnapshotShowMissingArguments(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		cli := test.NewTestCLI(t, server.App).
			WithServer(server).
			WithAccessKey([]auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		// Test missing both arguments
		err := cli.Run("database", "snapshot", "show")

		if err == nil {
			t.Error("expected error when no arguments provided, got none")
		}

		// Test missing timestamp argument
		testDatabase := test.MockDatabase(server.App)
		err = cli.Run("database", "snapshot", "show", fmt.Sprintf("%s/%s", testDatabase.DatabaseName, testDatabase.BranchName))

		if err == nil {
			t.Error("expected error when no timestamp provided, got none")
		}
	})
}

func TestDatabaseSnapshotShowAccessControl(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		testDatabase := test.MockDatabase(server.App)
		snapshotLogger := server.App.DatabaseManager.Resources(testDatabase.DatabaseID, testDatabase.DatabaseBranchID).SnapshotLogger()

		// Create a snapshot first
		db, err := server.App.DatabaseManager.ConnectionManager().Get(testDatabase.DatabaseID, testDatabase.DatabaseBranchID)

		if err != nil {
			t.Fatalf("failed to get database connection: %v", err)
		}

		defer server.App.DatabaseManager.ConnectionManager().Release(db)

		if err := db.GetConnection().Checkpoint(); err != nil {
			t.Fatalf("failed to create checkpoint: %v", err)
		}

		snapshots, err := snapshotLogger.GetSnapshots()

		if err != nil {
			t.Fatalf("failed to get snapshots: %v", err)
		}

		if len(snapshots) == 0 {
			t.Fatalf("expected at least one snapshot, got none")
		}

		var snapshot *backups.Snapshot
		for _, s := range snapshots {
			snapshot = s
			break
		}

		// Create CLI with limited access (similar to controller test pattern)
		cli := test.NewTestCLI(t, server.App).
			WithServer(server).
			WithAccessKey([]auth.Statement{
				{
					Effect:   auth.StatementEffectAllow,
					Resource: "*",
					Actions:  []auth.Privilege{auth.DatabasePrivilegeBackup},
				},
			})

		err = cli.Run("database", "snapshot", "show", fmt.Sprintf("%s/%s", testDatabase.DatabaseName, testDatabase.BranchName), fmt.Sprintf("%d", snapshot.Timestamp))

		if err != nil {
			t.Fatalf("expected no error with proper access, got %v", err)
		}

		// Should be able to see the snapshot details
		if cli.DoesNotSee("Database Snapshot") {
			t.Error("expected output to contain 'Database Snapshot' card title")
		}
	})
}
