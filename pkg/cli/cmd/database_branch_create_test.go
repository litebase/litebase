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

		primaryBranch, err := db.PrimaryBranch()

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
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

		primaryBranch, err := db.PrimaryBranch()

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
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

		primaryBranch, err := db.PrimaryBranch()

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
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

func TestDatabaseBranchCreate_CopiesSettingsFromParent(t *testing.T) {
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

		primaryBranch, err := db.PrimaryBranch()

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		// Update parent branch settings to custom values
		err = cli.Run("database", "branch", "settings", "update", fmt.Sprintf("%s/%s", mock.DatabaseName, primaryBranch.Name),
			"--backups-enabled=true",
			"--backup-interval=72h",
			"--backups-retention-days=21",
			"--incremental-backups-enabled=true",
			"--incremental-backups-retention-days=10",
			"--query-logs-enabled=true",
			"--query-logs-retention-days=45",
			"--error-logs-enabled=true",
			"--error-logs-retention-days=60",
		)

		if err != nil {
			t.Fatalf("expected no error updating settings, got %v", err)
		}

		// Create checkpoint
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

		// Create child branch
		err = cli.Run("database", "branch", "create", fmt.Sprintf("%s/main", mock.DatabaseName), "child-branch")

		if err != nil {
			t.Fatalf("expected no error creating child branch, got %v", err)
		}

		// Verify child branch was created
		if cli.DoesNotSee("child-branch") {
			t.Error("expected output to contain 'child-branch'")
		}

		// Get child branch settings to verify they match parent
		err = cli.Run("database", "branch", "settings", "show", fmt.Sprintf("%s/child-branch", mock.DatabaseName))

		if err != nil {
			t.Fatalf("expected no error showing child settings, got %v", err)
		}

		// Verify the settings were copied from parent
		if cli.DoesNotSee("72h") {
			t.Error("expected child branch to have backup interval '72h' from parent")
		}

		if cli.DoesNotSee("21") {
			t.Error("expected child branch to have backups retention days '21' from parent")
		}

		if cli.DoesNotSee("10") {
			t.Error("expected child branch to have incremental backups retention days '10' from parent")
		}

		if cli.DoesNotSee("45") {
			t.Error("expected child branch to have query logs retention days '45' from parent")
		}

		if cli.DoesNotSee("60") {
			t.Error("expected child branch to have error logs retention days '60' from parent")
		}

		// Verify through database API as well
		childBranch, err := db.Branch("child-branch")

		if err != nil {
			t.Fatalf("failed to get child branch: %v", err)
		}

		childSettings, err := childBranch.GetBranchSettings()

		if err != nil {
			t.Fatalf("failed to get child branch settings: %v", err)
		}

		parentSettings, err := primaryBranch.GetBranchSettings()

		if err != nil {
			t.Fatalf("failed to get parent branch settings: %v", err)
		}

		// Verify all settings match
		if childSettings.BackupsEnabled != parentSettings.BackupsEnabled {
			t.Errorf("expected BackupsEnabled to match parent (%v), got %v", parentSettings.BackupsEnabled, childSettings.BackupsEnabled)
		}

		if childSettings.BackupInterval != parentSettings.BackupInterval {
			t.Errorf("expected BackupInterval to match parent (%v), got %v", parentSettings.BackupInterval, childSettings.BackupInterval)
		}

		if childSettings.BackupsRetentionDays != parentSettings.BackupsRetentionDays {
			t.Errorf("expected BackupsRetentionDays to match parent (%v), got %v", parentSettings.BackupsRetentionDays, childSettings.BackupsRetentionDays)
		}

		if childSettings.IncrementalBackupsEnabled != parentSettings.IncrementalBackupsEnabled {
			t.Errorf("expected IncrementalBackupsEnabled to match parent (%v), got %v", parentSettings.IncrementalBackupsEnabled, childSettings.IncrementalBackupsEnabled)
		}

		if childSettings.IncrementalBackupsRetentionDays != parentSettings.IncrementalBackupsRetentionDays {
			t.Errorf("expected IncrementalBackupsRetentionDays to match parent (%v), got %v", parentSettings.IncrementalBackupsRetentionDays, childSettings.IncrementalBackupsRetentionDays)
		}

		if childSettings.QueryLogsEnabled != parentSettings.QueryLogsEnabled {
			t.Errorf("expected QueryLogsEnabled to match parent (%v), got %v", parentSettings.QueryLogsEnabled, childSettings.QueryLogsEnabled)
		}

		if childSettings.QueryLogsRetentionDays != parentSettings.QueryLogsRetentionDays {
			t.Errorf("expected QueryLogsRetentionDays to match parent (%v), got %v", parentSettings.QueryLogsRetentionDays, childSettings.QueryLogsRetentionDays)
		}

		if childSettings.ErrorLogsEnabled != parentSettings.ErrorLogsEnabled {
			t.Errorf("expected ErrorLogsEnabled to match parent (%v), got %v", parentSettings.ErrorLogsEnabled, childSettings.ErrorLogsEnabled)
		}

		if childSettings.ErrorLogsRetentionDays != parentSettings.ErrorLogsRetentionDays {
			t.Errorf("expected ErrorLogsRetentionDays to match parent (%v), got %v", parentSettings.ErrorLogsRetentionDays, childSettings.ErrorLogsRetentionDays)
		}
	})
}
