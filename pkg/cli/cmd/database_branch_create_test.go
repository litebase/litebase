package cmd_test

import (
	"fmt"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
)

func TestDatabaseBranchCreate(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		cli := test.NewTestCLI(t, server.App).
			WithServer(server).
			WithAccessKey([]auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		err := cli.Run("database", "create", "test-db")

		if err != nil {
			t.Fatalf("expected no error creating database, got %v", err)
		}

		db, err := server.App.DatabaseManager.GetByName("test-db")

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

		err = cli.Run("database", "branch", "create", "test-db/main", "feature-branch")

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if cli.DoesNotSee("Database Branch") {
			t.Error("expected output to contain 'Database Branch'")
		}

		if cli.DoesNotSee("Name") {
			t.Error("expected output to contain 'Name'")
		}

		if cli.DoesNotSee("feature-branch") {
			t.Error("expected output to contain 'feature-branch'")
		}

		if cli.DoesNotSee("Database ID") {
			t.Error("expected output to contain 'Database ID'")
		}

		if cli.DoesNotSee("Created At") {
			t.Error("expected output to contain 'Created At'")
		}

		if cli.DoesNotSee("Updated At") {
			t.Error("expected output to contain 'Updated At'")
		}
	})
}

func TestDatabaseBranchCreateWithParentBranch(t *testing.T) {
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

		err = cli.Run("database", "branch", "create", fmt.Sprintf("%s/main", mock.DatabaseName), "dev-branch")

		if err != nil {
			t.Fatalf("expected no error creating first branch, got %v", err)
		}

		devBranchObj, err := db.Branch("dev-branch")

		if err != nil {
			t.Fatalf("expected to get dev branch, got error: %v", err)
		}

		devCon, err := server.App.DatabaseManager.ConnectionManager().Get(db.DatabaseID, devBranchObj.DatabaseBranchID)

		if err != nil {
			t.Fatalf("failed to get dev branch connection: %v", err)
		}

		defer server.App.DatabaseManager.ConnectionManager().Release(devCon)

		if err := devCon.Checkpoint(); err != nil {
			t.Fatalf("failed to create checkpoint for dev branch: %v", err)
		}

		err = cli.Run("database", "branch", "create", fmt.Sprintf("%s/dev-branch", mock.DatabaseName), "feature-branch")

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if cli.DoesNotSee("Database Branch") {
			t.Error("expected output to contain 'Database Branch'")
		}

		if cli.DoesNotSee("feature-branch") {
			t.Error("expected output to contain 'feature-branch'")
		}

		if cli.DoesNotSee("Parent Branch") {
			t.Error("expected output to contain 'Parent Branch'")
		}

		if cli.DoesNotSee("dev-branch") {
			t.Error("expected output to contain 'dev-branch'")
		}

		db, err = server.App.DatabaseManager.Get(mock.DatabaseID)

		if err != nil {
			t.Fatalf("expected to get database, got error: %v", err)
		}

		branch, err := db.Branch("feature-branch")

		if err != nil {
			t.Fatalf("expected to get branch, got error: %v", err)
		}

		parentBranch := branch.ParentBranch()

		if parentBranch == nil {
			t.Error("expected parent branch to be set")
		} else if parentBranch.Name != "dev-branch" {
			t.Errorf("expected parent branch to be 'dev-branch', got '%s'", parentBranch.Name)
		}
	})
}

func TestDatabaseBranchCreateMissingBranchName(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		cli := test.NewTestCLI(t, server.App).
			WithServer(server).
			WithAccessKey([]auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		err := cli.Run("database", "create", "test-db")

		if err != nil {
			t.Fatalf("expected no error creating database, got %v", err)
		}

		err = cli.Run("database", "branch", "create", "test-db/main")

		if err == nil {
			t.Error("expected error when branch name is missing, got none")
		}
	})
}

func TestDatabaseBranchCreateInvalidDatabaseName(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		cli := test.NewTestCLI(t, server.App).
			WithServer(server).
			WithAccessKey([]auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		err := cli.Run("database", "branch", "create", "non-existent-db/main", "feature-branch")

		if err == nil {
			t.Error("expected error when database doesn't exist, got none")
		}
	})
}

func TestDatabaseBranchCreateDuplicateBranchName(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		cli := test.NewTestCLI(t, server.App).
			WithServer(server).
			WithAccessKey([]auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		err := cli.Run("database", "create", "test-db")

		if err != nil {
			t.Fatalf("expected no error creating database, got %v", err)
		}

		db, err := server.App.DatabaseManager.GetByName("test-db")

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

		err = cli.Run("database", "branch", "create", "test-db/main", "feature-branch")

		if err != nil {
			t.Fatalf("expected no error creating first branch, got %v", err)
		}

		err = cli.Run("database", "branch", "create", "test-db/main", "feature-branch")

		if err == nil {
			t.Error("expected error when creating duplicate branch, got none")
		}
	})
}
