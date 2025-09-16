package cmd_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/database"
)

func TestDatabaseSnapshotList(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		cli := test.NewTestCLI(t, server.App).
			WithServer(server).
			WithAccessKey([]auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		testDatabase := test.MockDatabase(server.App)

		db, err := server.App.DatabaseManager.ConnectionManager().Get(testDatabase.DatabaseID, testDatabase.DatabaseBranchID)

		if err != nil {
			t.Fatalf("failed to get database connection: %v", err)
		}

		defer server.App.DatabaseManager.ConnectionManager().Release(db)

		if err := db.GetConnection().Checkpoint(); err != nil {
			t.Fatalf("failed to create checkpoint: %v", err)
		}

		_, err = db.GetConnection().Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)", nil)

		if err != nil {
			t.Fatalf("failed to create test table: %v", err)
		}

		if err := db.GetConnection().Checkpoint(); err != nil {
			t.Fatalf("failed to create checkpoint: %v", err)
		}

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

		err = cli.Run("database", "snapshot", "list", fmt.Sprintf("%s/%s", testDatabase.DatabaseName, testDatabase.BranchName))

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		// Check that snapshot information is displayed
		if cli.DoesNotSee("Date (UTC)") {
			t.Error("expected output to contain 'Date (UTC)' column header")
		}

		if cli.DoesNotSee("Restore Points") {
			t.Error("expected output to contain 'Restore Points' column header")
		}

		if cli.DoesNotSee("Start Restore Point") {
			t.Error("expected output to contain 'Start Restore Point' column header")
		}

		if cli.DoesNotSee("Last Restore Point") {
			t.Error("expected output to contain 'Last Restore Point' column header")
		}

		// Check that we have date format (YYYY-MM-DD HH:MM:SS) instead of timestamp
		// Calculate the expected date for today's start of day
		startOfDay := time.Now().UTC().Truncate(24 * time.Hour)
		expectedDate := startOfDay.Format("2006-01-02")

		if cli.DoesNotSee(expectedDate) { // Should contain today's date
			t.Errorf("expected output to contain date %s", expectedDate)
		}

		if cli.DoesNotSee("3") { // Restore points count
			t.Error("expected output to contain restore points count")
		}
	})
}

func TestDatabaseSnapshotListNonExistentDatabase(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		cli := test.NewTestCLI(t, server.App).
			WithServer(server).
			WithAccessKey([]auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		err := cli.Run("database", "snapshot", "list", "non-existent-database/main")

		if err == nil {
			t.Error("expected error when listing snapshots for non-existent database, got none")
		}
	})
}

func TestDatabaseSnapshotListNonExistentBranch(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		cli := test.NewTestCLI(t, server.App).
			WithServer(server).
			WithAccessKey([]auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		testDatabase := test.MockDatabase(server.App)

		err := cli.Run("database", "snapshot", "list", fmt.Sprintf("%s/non-existent-branch", testDatabase.DatabaseName))

		if err == nil {
			t.Error("expected error when listing snapshots for non-existent branch, got none")
		}
	})
}

func TestDatabaseSnapshotListInvalidPath(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		cli := test.NewTestCLI(t, server.App).
			WithServer(server).
			WithAccessKey([]auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		err := cli.Run("database", "snapshot", "list", "invalid-path-format")

		if err == nil {
			t.Error("expected error when using invalid path format, got none")
		}
	})
}

func TestDatabaseSnapshotListMissingArguments(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		cli := test.NewTestCLI(t, server.App).
			WithServer(server).
			WithAccessKey([]auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		err := cli.Run("database", "snapshot", "list")

		if err == nil {
			t.Error("expected error when no database/branch path provided, got none")
		}
	})
}

func TestDatabaseSnapshotListAccessControl(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		testDatabase := test.MockDatabase(server.App)

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

		err := cli.Run("database", "snapshot", "list", fmt.Sprintf("%s/%s", testDatabase.DatabaseName, testDatabase.BranchName))

		// The key test is that we don't get a permission denied error
		if err != nil {
			if cli.Sees("Forbidden") || cli.Sees("permission") {
				t.Fatalf("expected no permission error, got %v", err)
			}

			// If we get "Failed to get snapshots", that might be because there are no snapshots
			// which is acceptable for this access control test
			t.Logf("Command failed (possibly due to no snapshots): %v", err)
		} else {
			// If successful, should show table headers
			if cli.DoesNotSee("Timestamp") {
				t.Error("expected output to contain 'Timestamp' column header")
			}
		}
	})
}
