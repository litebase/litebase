package cmd_test

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/backups"
	"github.com/litebase/litebase/pkg/database"
)

func TestDatabaseRestoreSuccess(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		sourceDB := test.MockDatabase(server.App)
		snapshotLogger := server.App.DatabaseManager.Resources(sourceDB.DatabaseID, sourceDB.DatabaseBranchID).SnapshotLogger()

		db, err := server.App.DatabaseManager.ConnectionManager().Get(sourceDB.DatabaseID, sourceDB.DatabaseBranchID)

		if err != nil {
			t.Fatalf("failed to get database connection: %v", err)
		}

		defer server.App.DatabaseManager.ConnectionManager().Release(db)

		// Create initial checkpoint
		if err := db.GetConnection().Checkpoint(); err != nil {
			t.Fatalf("failed to create checkpoint: %v", err)
		}

		// Create a test table and insert data
		_, err = db.GetConnection().Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)", nil)

		if err != nil {
			t.Fatalf("failed to create test table: %v", err)
		}

		if err := db.GetConnection().Checkpoint(); err != nil {
			t.Fatalf("failed to create checkpoint: %v", err)
		}

		// Insert test data in a transaction
		err = db.GetConnection().Transaction(false, func(db *database.DatabaseConnection) error {
			_, err = db.Exec("INSERT INTO test (value) VALUES ('restore test data')", nil)
			return err
		})

		if err != nil {
			t.Fatalf("failed to insert data: %v", err)
		}

		// Create final checkpoint to establish restore point
		if err := db.GetConnection().Checkpoint(); err != nil {
			t.Fatalf("failed to create final checkpoint: %v", err)
		}

		// Get the snapshots to find a restore point timestamp
		snapshots, err := snapshotLogger.GetSnapshots()

		if err != nil {
			t.Fatalf("failed to get snapshots: %v", err)
		}

		if len(snapshots) == 0 {
			t.Fatal("no snapshots available")
		}

		// Use the first snapshot's timestamp
		var snapshot *backups.Snapshot

		for _, s := range snapshots {
			snapshot = s
			break
		}

		timestamp := snapshot.Timestamp

		// Create target database
		targetDB := test.MockDatabase(server.App)

		cli := test.NewTestCLI(t, server.App).
			WithServer(server).
			WithAccessKey([]auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		err = cli.Run(
			"database", "restore",
			fmt.Sprintf("%s/%s", sourceDB.DatabaseName, sourceDB.BranchName),
			fmt.Sprintf("%s/%s", targetDB.DatabaseName, targetDB.BranchName),
			"--timestamp", strconv.FormatInt(timestamp, 10),
		)

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if cli.DoesNotSee("Database restore completed successfully") {
			t.Error("expected output to contain success message")
		}

		if cli.DoesNotSee(fmt.Sprintf("%s/%s", sourceDB.DatabaseName, sourceDB.BranchName)) {
			t.Error("expected output to contain source database name")
		}

		if cli.DoesNotSee(fmt.Sprintf("%s/%s", targetDB.DatabaseName, targetDB.BranchName)) {
			t.Error("expected output to contain target database name")
		}

		if cli.DoesNotSee(strconv.FormatInt(timestamp, 10)) {
			t.Error("expected output to contain timestamp")
		}
	})
}

func TestDatabaseRestoreMissingFlags(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		sourceDB := test.MockDatabase(server.App)
		targetDB := test.MockDatabase(server.App)

		cli := test.NewTestCLI(t, server.App).
			WithServer(server).
			WithAccessKey([]auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		err := cli.Run(
			"database", "restore",
			fmt.Sprintf("%s/%s", sourceDB.DatabaseName, sourceDB.BranchName),
			fmt.Sprintf("%s/%s", targetDB.DatabaseName, targetDB.BranchName),
		)

		if err == nil {
			t.Error("expected error when timestamp flag is missing, got none")
		}
	})
}

func TestDatabaseRestoreInvalidTimestamp(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		sourceDB := test.MockDatabase(server.App)
		targetDB := test.MockDatabase(server.App)

		cli := test.NewTestCLI(t, server.App).
			WithServer(server).
			WithAccessKey([]auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		err := cli.Run(
			"database", "restore",
			fmt.Sprintf("%s/%s", sourceDB.DatabaseName, sourceDB.BranchName),
			fmt.Sprintf("%s/%s", targetDB.DatabaseName, targetDB.BranchName),
			"--timestamp", "invalid-timestamp",
		)

		if err == nil {
			t.Error("expected error when timestamp format is invalid, got none")
		}

		// Test empty timestamp
		err = cli.Run(
			"database", "restore",
			fmt.Sprintf("%s/%s", sourceDB.DatabaseName, sourceDB.BranchName),
			fmt.Sprintf("%s/%s", targetDB.DatabaseName, targetDB.BranchName),
			"--timestamp", "",
		)

		if err == nil {
			t.Error("expected error when timestamp is empty, got none")
		}
	})
}

func TestDatabaseRestoreInvalidDatabasePath(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		targetDB := test.MockDatabase(server.App)

		cli := test.NewTestCLI(t, server.App).
			WithServer(server).
			WithAccessKey([]auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		err := cli.Run(
			"database", "restore", "invalid-path",
			fmt.Sprintf("%s/%s", targetDB.DatabaseName, targetDB.BranchName),
			"--timestamp", "1699123456789012345",
		)

		if err == nil {
			t.Error("expected error when source database path is invalid, got none")
		}

		err = cli.Run(
			"database", "restore",
			fmt.Sprintf("%s/%s", targetDB.DatabaseName, targetDB.BranchName),
			"invalid-target-path",
			"--timestamp", "1699123456789012345",
		)

		if err == nil {
			t.Error("expected error when target database path is invalid, got none")
		}
	})
}

func TestDatabaseRestoreNonExistentSourceDatabase(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		targetDB := test.MockDatabase(server.App)

		cli := test.NewTestCLI(t, server.App).
			WithServer(server).
			WithAccessKey([]auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		err := cli.Run(
			"database", "restore", "non-existent-db/main",
			fmt.Sprintf("%s/%s", targetDB.DatabaseName, targetDB.BranchName),
			"--timestamp", "1699123456789012345",
		)

		if err == nil {
			t.Error("expected error when source database does not exist, got none")
		}
	})
}

func TestDatabaseRestoreInsufficientPrivileges(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		sourceDB := test.MockDatabase(server.App)
		targetDB := test.MockDatabase(server.App)

		cli := test.NewTestCLI(t, server.App).
			WithServer(server).
			WithAccessKey([]auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{auth.DatabasePrivilegeRead}},
			})

		err := cli.Run(
			"database", "restore",
			fmt.Sprintf("%s/%s", sourceDB.DatabaseName, sourceDB.BranchName),
			fmt.Sprintf("%s/%s", targetDB.DatabaseName, targetDB.BranchName),
			"--timestamp", "1699123456789012345",
		)

		if err == nil {
			t.Error("expected error when user lacks restore privileges, got none")
		}
	})
}

func TestDatabaseRestoreWrongArgumentCount(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		cli := test.NewTestCLI(t, server.App).
			WithServer(server).
			WithAccessKey([]auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		err := cli.Run("database", "restore")

		if err == nil {
			t.Error("expected error when no arguments provided, got none")
		}

		err = cli.Run("database", "restore", "source-db/main")

		if err == nil {
			t.Error("expected error when only one argument provided, got none")
		}

		err = cli.Run(
			"database", "restore", "source-db/main", "target-db/main", "extra-arg",
			"--timestamp", "1699123456789012345",
		)

		if err == nil {
			t.Error("expected error when too many arguments provided, got none")
		}
	})
}
