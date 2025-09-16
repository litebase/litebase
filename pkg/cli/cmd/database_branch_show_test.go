package cmd_test

import (
	"fmt"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
)

func TestDatabaseBranchShow(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		cli := test.NewTestCLI(t, server.App).
			WithServer(server).
			WithAccessKey([]auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		testDatabase := test.MockDatabase(server.App)

		db, err := server.App.DatabaseManager.Get(testDatabase.DatabaseID)

		if err != nil {
			t.Fatalf("failed to get mock database: %v", err)
		}

		con, err := server.App.DatabaseManager.ConnectionManager().Get(testDatabase.DatabaseID, testDatabase.DatabaseBranchID)

		if err != nil {
			t.Fatalf("failed to get database connection: %v", err)
		}

		defer server.App.DatabaseManager.ConnectionManager().Release(con)

		if _, err := con.GetConnection().Exec("CREATE TABLE IF NOT EXISTS test_table (id INTEGER PRIMARY KEY, name TEXT)", nil); err != nil {
			t.Fatalf("failed to create test table: %v", err)
		}

		if err := con.Checkpoint(); err != nil {
			t.Fatalf("failed to create checkpoint: %v", err)
		}

		primaryBranch := db.PrimaryBranch()

		if primaryBranch == nil {
			t.Fatalf("expected primary branch to be found, got nil")
		}

		err = cli.Run("database", "branch", "show", fmt.Sprintf("%s/%s", testDatabase.DatabaseName, primaryBranch.Name))

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if cli.DoesNotSee(primaryBranch.Name) {
			t.Error("expected output to contain branch name")
		}

		if cli.DoesNotSee(primaryBranch.DatabaseBranchID) {
			t.Error("expected output to contain branch ID")
		}

		if cli.DoesNotSee(testDatabase.DatabaseID) {
			t.Error("expected output to contain database ID")
		}

		if cli.DoesNotSee("Database Branch") {
			t.Error("expected output to contain 'Database Branch' card title")
		}
	})
}

func TestDatabaseBranchShowCreatedBranch(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		cli := test.NewTestCLI(t, server.App).
			WithServer(server).
			WithAccessKey([]auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		testDatabase := test.MockDatabase(server.App)

		db, err := server.App.DatabaseManager.Get(testDatabase.DatabaseID)

		if err != nil {
			t.Fatalf("failed to get mock database: %v", err)
		}

		con, err := server.App.DatabaseManager.ConnectionManager().Get(testDatabase.DatabaseID, testDatabase.DatabaseBranchID)

		if err != nil {
			t.Fatalf("failed to get database connection: %v", err)
		}

		defer server.App.DatabaseManager.ConnectionManager().Release(con)

		if _, err := con.GetConnection().Exec("CREATE TABLE IF NOT EXISTS test_table (id INTEGER PRIMARY KEY, name TEXT)", nil); err != nil {
			t.Fatalf("failed to create test table: %v", err)
		}

		if err := con.Checkpoint(); err != nil {
			t.Fatalf("failed to create checkpoint: %v", err)
		}

		testBranch, err := db.CreateBranch("test-branch", "main")

		if err != nil {
			t.Fatalf("failed to create test branch: %v", err)
		}

		err = cli.Run("database", "branch", "show", fmt.Sprintf("%s/%s", testDatabase.DatabaseName, testBranch.Name))

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if cli.DoesNotSee("test-branch") {
			t.Error("expected output to contain branch name 'test-branch'")
		}

		if cli.DoesNotSee(testBranch.DatabaseBranchID) {
			t.Error("expected output to contain branch ID")
		}

		if cli.DoesNotSee(testDatabase.DatabaseID) {
			t.Error("expected output to contain database ID")
		}

		if cli.DoesNotSee("main") {
			t.Error("expected output to contain parent branch 'main'")
		}

		if cli.DoesNotSee("Database Branch") {
			t.Error("expected output to contain 'Database Branch' card title")
		}

		if cli.DoesNotSee("Parent Branch") {
			t.Error("expected output to contain 'Parent Branch' field")
		}
	})
}

func TestDatabaseBranchShowNonExistentBranch(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		cli := test.NewTestCLI(t, server.App).
			WithServer(server).
			WithAccessKey([]auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		testDatabase := test.MockDatabase(server.App)

		err := cli.Run("database", "branch", "show", fmt.Sprintf("%s/non-existent-branch", testDatabase.DatabaseName))

		if err == nil {
			t.Error("expected error when showing non-existent branch, got none")
		}
	})
}

func TestDatabaseBranchShowNonExistentDatabase(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		cli := test.NewTestCLI(t, server.App).
			WithServer(server).
			WithAccessKey([]auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		err := cli.Run("database", "branch", "show", "non-existent-database/main")

		if err == nil {
			t.Error("expected error when showing branch from non-existent database, got none")
		}
	})
}

func TestDatabaseBranchShowInvalidPath(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		cli := test.NewTestCLI(t, server.App).
			WithServer(server).
			WithAccessKey([]auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		err := cli.Run("database", "branch", "show", "invalid-path-format")

		if err == nil {
			t.Error("expected error when using invalid path format, got none")
		}
	})
}

func TestDatabaseBranchShowMissingArguments(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		cli := test.NewTestCLI(t, server.App).
			WithServer(server).
			WithAccessKey([]auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		err := cli.Run("database", "branch", "show")

		if err == nil {
			t.Error("expected error when no database/branch path provided, got none")
		}
	})
}

func TestDatabaseBranchShowAccessControl(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		testDatabase := test.MockDatabase(server.App)

		db, err := server.App.DatabaseManager.Get(testDatabase.DatabaseID)

		if err != nil {
			t.Fatalf("failed to get mock database: %v", err)
		}

		primaryBranch := db.PrimaryBranch()

		if primaryBranch == nil {
			t.Fatalf("expected primary branch to be found, got nil")
		}

		cli := test.NewTestCLI(t, server.App).
			WithServer(server).
			WithAccessKey([]auth.Statement{
				{
					Effect:   auth.StatementEffectAllow,
					Resource: auth.Resource(fmt.Sprintf("database:%s:branch:*", testDatabase.DatabaseID)),
					Actions:  []auth.Privilege{auth.DatabasePrivilegeShow},
				},
			})

		err = cli.Run("database", "branch", "show", fmt.Sprintf("%s/%s", testDatabase.DatabaseName, primaryBranch.Name))

		if err != nil {
			t.Fatalf("expected no error with proper access, got %v", err)
		}

		if cli.DoesNotSee(primaryBranch.Name) {
			t.Error("expected output to contain branch name")
		}

		if cli.DoesNotSee("Database Branch") {
			t.Error("expected output to contain 'Database Branch' card title")
		}
	})
}
