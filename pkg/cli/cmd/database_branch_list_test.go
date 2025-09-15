package cmd_test

import (
	"fmt"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
)

func TestDatabaseBranchList(t *testing.T) {
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

		branch1, err := db.CreateBranch("branch-1", "main")

		if err != nil {
			t.Fatalf("failed to create branch-1: %v", err)
		}

		branch2, err := db.CreateBranch("branch-2", "main")

		if err != nil {
			t.Fatalf("failed to create branch-2: %v", err)
		}

		branch3, err := db.CreateBranch("branch-3", "main")

		if err != nil {
			t.Fatalf("failed to create branch-3: %v", err)
		}

		err = cli.Run("database", "branch", "list", testDatabase.DatabaseName)

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if cli.DoesNotSee("main") {
			t.Error("expected output to contain primary branch 'main'")
		}

		if cli.DoesNotSee("branch-1") {
			t.Error("expected output to contain 'branch-1'")
		}

		if cli.DoesNotSee("branch-2") {
			t.Error("expected output to contain 'branch-2'")
		}

		if cli.DoesNotSee("branch-3") {
			t.Error("expected output to contain 'branch-3'")
		}

		if cli.DoesNotSee(branch1.DatabaseBranchID) {
			t.Error("expected output to contain branch-1 ID")
		}

		if cli.DoesNotSee(branch2.DatabaseBranchID) {
			t.Error("expected output to contain branch-2 ID")
		}

		if cli.DoesNotSee(branch3.DatabaseBranchID) {
			t.Error("expected output to contain branch-3 ID")
		}
	})
}

func TestDatabaseBranchListNonExistentDatabase(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		cli := test.NewTestCLI(t, server.App).
			WithServer(server).
			WithAccessKey([]auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		err := cli.Run("database", "branch", "list", "non-existent-database")

		if err == nil {
			t.Error("expected error when listing branches for non-existent database, got none")
		}
	})
}

func TestDatabaseBranchListMissingArguments(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		cli := test.NewTestCLI(t, server.App).
			WithServer(server).
			WithAccessKey([]auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		err := cli.Run("database", "branch", "list")

		if err == nil {
			t.Error("expected error when no database name provided, got none")
		}
	})
}

func TestDatabaseBranchListWithParentBranches(t *testing.T) {
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

		parentBranch, err := db.CreateBranch("feature-branch", "main")

		if err != nil {
			t.Fatalf("failed to create parent branch: %v", err)
		}

		parentCon, err := server.App.DatabaseManager.ConnectionManager().Get(testDatabase.DatabaseID, parentBranch.DatabaseBranchID)

		if err != nil {
			t.Fatalf("failed to get parent branch connection: %v", err)
		}

		defer server.App.DatabaseManager.ConnectionManager().Release(parentCon)

		if err := parentCon.Checkpoint(); err != nil {
			t.Fatalf("failed to create checkpoint on parent branch: %v", err)
		}

		childBranch, err := db.CreateBranch("child-branch", "feature-branch")

		if err != nil {
			t.Fatalf("failed to create child branch: %v", err)
		}

		err = cli.Run("database", "branch", "list", testDatabase.DatabaseName)

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if cli.DoesNotSee("feature-branch") {
			t.Error("expected output to contain parent branch 'feature-branch'")
		}

		if cli.DoesNotSee("child-branch") {
			t.Error("expected output to contain child branch 'child-branch'")
		}

		output := cli.GetOutput()

		if output == "" {
			t.Error("expected some output from branch list command")
		}

		if cli.DoesNotSee(parentBranch.DatabaseBranchID) {
			t.Error("expected output to contain parent branch ID")
		}

		if cli.DoesNotSee(childBranch.DatabaseBranchID) {
			t.Error("expected output to contain child branch ID")
		}
	})
}

func TestDatabaseBranchListAccessControl(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		testDatabase := test.MockDatabase(server.App)

		cli := test.NewTestCLI(t, server.App).
			WithServer(server).
			WithAccessKey([]auth.Statement{
				{
					Effect:   auth.StatementEffectAllow,
					Resource: auth.Resource(fmt.Sprintf("database:%s", testDatabase.DatabaseID)),
					Actions:  []auth.Privilege{auth.DatabaseBranchPrivilegeList},
				},
			})

		err := cli.Run("database", "branch", "list", testDatabase.DatabaseName)

		if err != nil {
			t.Fatalf("expected no error with proper access, got %v", err)
		}

		if cli.DoesNotSee("main") {
			t.Error("expected output to contain primary branch 'main'")
		}
	})
}
