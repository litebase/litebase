package cmd_test

import (
	"fmt"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
)

func TestDatabaseBranchDelete(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		cli := test.NewTestCLI(t, server.App).
			WithServer(server).
			WithAccessKey([]auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		mock := test.MockDatabase(server.App)

		db, err := server.App.DatabaseManager.Get(mock.DatabaseID)

		if err != nil {
			t.Fatalf("expected to get database, got error: %v", err)
		}

		primaryBranch := db.PrimaryBranch()

		if primaryBranch == nil {
			t.Fatalf("expected primary branch to exist")
		}

		con, err := server.App.DatabaseManager.ConnectionManager().Get(db.DatabaseID, primaryBranch.DatabaseBranchID)

		if err != nil {
			t.Fatalf("failed to get database connection: %v", err)
		}

		defer server.App.DatabaseManager.ConnectionManager().Release(con)

		if _, err := con.GetConnection().Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)", nil); err != nil {
			t.Fatalf("failed to create test table: %v", err)
		}

		if err := con.Checkpoint(); err != nil {
			t.Fatalf("failed to create checkpoint: %v", err)
		}

		err = cli.Run("database", "branch", "create", fmt.Sprintf("%s/main", mock.DatabaseName), "test-branch")

		if err != nil {
			t.Fatalf("expected no error creating branch, got %v", err)
		}

		_, err = db.Branch("test-branch")

		if err != nil {
			t.Fatalf("expected branch to exist, got error: %v", err)
		}

		err = cli.Run("database", "branch", "delete", fmt.Sprintf("%s/test-branch", mock.DatabaseName))

		if err != nil {
			t.Fatalf("expected no error deleting branch, got %v", err)
		}

		if cli.DoesNotSee("Database branch deleted successfully") {
			t.Error("expected output to contain success message")
		}

		_, err = db.Branch("test-branch")

		if err == nil {
			t.Error("expected branch to be deleted but it still exists")
		}
	})
}

func TestDatabaseBranchDeletePrimaryBranch(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		cli := test.NewTestCLI(t, server.App).
			WithServer(server).
			WithAccessKey([]auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		mock := test.MockDatabase(server.App)

		err := cli.Run("database", "branch", "delete", fmt.Sprintf("%s/main", mock.DatabaseName))

		if err == nil {
			t.Error("expected error when deleting primary branch, got none")
		}
	})
}

func TestDatabaseBranchDeleteNonExistentBranch(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		cli := test.NewTestCLI(t, server.App).
			WithServer(server).
			WithAccessKey([]auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		mock := test.MockDatabase(server.App)

		err := cli.Run("database", "branch", "delete", fmt.Sprintf("%s/non-existent-branch", mock.DatabaseName))

		if err == nil {
			t.Error("expected error when deleting non-existent branch, got none")
		}
	})
}

func TestDatabaseBranchDeleteNonExistentDatabase(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		cli := test.NewTestCLI(t, server.App).
			WithServer(server).
			WithAccessKey([]auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		err := cli.Run("database", "branch", "delete", "non-existent-db/some-branch")

		if err == nil {
			t.Error("expected error when deleting branch from non-existent database, got none")
		}
	})
}

func TestDatabaseBranchDeleteInvalidPath(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		cli := test.NewTestCLI(t, server.App).
			WithServer(server).
			WithAccessKey([]auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		err := cli.Run("database", "branch", "delete", "invalid-path")

		if err == nil {
			t.Error("expected error when using invalid path format, got none")
		}
	})
}

func TestDatabaseBranchDeleteMissingArguments(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		cli := test.NewTestCLI(t, server.App).
			WithServer(server).
			WithAccessKey([]auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		err := cli.Run("database", "branch", "delete")

		if err == nil {
			t.Error("expected error when no arguments provided, got none")
		}
	})
}
