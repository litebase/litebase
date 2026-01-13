package cmd_test

import (
	"fmt"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
)

func TestDatabaseBranchSettingsShow(t *testing.T) {
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

		primaryBranch, err := db.PrimaryBranch()

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		err = cli.Run("database", "branch", "settings", "show", fmt.Sprintf("%s/%s", testDatabase.DatabaseName, primaryBranch.Name))

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if cli.DoesNotSee("Branch Settings") {
			t.Error("expected output to contain 'Branch Settings' card title")
		}

		if cli.DoesNotSee("Backups Enabled") {
			t.Error("expected output to contain 'Backups Enabled'")
		}

		if cli.DoesNotSee("Incremental Backups Enabled") {
			t.Error("expected output to contain 'Incremental Backups Enabled'")
		}

		if cli.DoesNotSee("Query Logs Enabled") {
			t.Error("expected output to contain 'Query Logs Enabled'")
		}

		if cli.DoesNotSee("Error Logs Enabled") {
			t.Error("expected output to contain 'Error Logs Enabled'")
		}
	})
}

func TestDatabaseBranchSettingsShowInvalidPath(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		cli := test.NewTestCLI(t, server.App).
			WithServer(server).
			WithAccessKey([]auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		// Test with invalid database path (no slash)
		err := cli.Run("database", "branch", "settings", "show", "invalid-path")

		if err == nil {
			t.Error("expected error for invalid database path")
		}
	})
}

func TestDatabaseBranchSettingsShowNonExistentDatabase(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		cli := test.NewTestCLI(t, server.App).
			WithServer(server).
			WithAccessKey([]auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		// Test with non-existent database
		err := cli.Run("database", "branch", "settings", "show", "non-existent/main")

		if err == nil {
			t.Error("expected error for non-existent database")
		}
	})
}

func TestDatabaseBranchSettingsShowWithBackupsEnabled(t *testing.T) {
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

		primaryBranch, err := db.PrimaryBranch()

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		// First, enable backups using the update command
		err = cli.Run("database", "branch", "settings", "update", fmt.Sprintf("%s/%s", testDatabase.DatabaseName, primaryBranch.Name),
			"--backups-enabled=true",
			"--backup-interval=24h",
			"--backups-retention-days=7",
			"--incremental-backups-enabled=false",
			"--incremental-backups-retention-days=3",
			"--query-logs-enabled=false",
			"--query-logs-retention-days=7",
			"--error-logs-enabled=false",
			"--error-logs-retention-days=7",
		)

		if err != nil {
			t.Fatalf("expected no error updating settings, got %v", err)
		}

		// Now show the settings
		err = cli.Run("database", "branch", "settings", "show", fmt.Sprintf("%s/%s", testDatabase.DatabaseName, primaryBranch.Name))

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if cli.DoesNotSee("24h") {
			t.Error("expected output to contain backup interval '24h'")
		}

		if cli.DoesNotSee("7") {
			t.Error("expected output to contain retention days '7'")
		}
	})
}
