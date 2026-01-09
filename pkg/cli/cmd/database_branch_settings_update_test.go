package cmd_test

import (
	"fmt"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
)

func TestDatabaseBranchSettingsUpdate(t *testing.T) {
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

		// Test non-interactive mode with flags
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
			t.Fatalf("expected no error, got %v", err)
		}

		if cli.DoesNotSee("Branch Settings") {
			t.Error("expected output to contain 'Branch Settings'")
		}

		if cli.DoesNotSee("Backups Enabled") {
			t.Error("expected output to contain 'Backups Enabled'")
		}

		if cli.DoesNotSee("24h") {
			t.Error("expected output to contain backup interval '24h'")
		}

		if cli.DoesNotSee("7") {
			t.Error("expected output to contain retention days")
		}
	})
}

func TestDatabaseBranchSettingsUpdateEnableAllFeatures(t *testing.T) {
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

		// Enable all features
		err = cli.Run("database", "branch", "settings", "update", fmt.Sprintf("%s/%s", testDatabase.DatabaseName, primaryBranch.Name),
			"--backups-enabled=true",
			"--backup-interval=48h",
			"--backups-retention-days=14",
			"--incremental-backups-enabled=true",
			"--incremental-backups-retention-days=7",
			"--query-logs-enabled=true",
			"--query-logs-retention-days=30",
			"--error-logs-enabled=true",
			"--error-logs-retention-days=90",
		)

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if cli.DoesNotSee("48h") {
			t.Error("expected output to contain backup interval '48h'")
		}

		if cli.DoesNotSee("14") {
			t.Error("expected output to contain backups retention days '14'")
		}

		if cli.DoesNotSee("30") {
			t.Error("expected output to contain query logs retention days '30'")
		}

		if cli.DoesNotSee("90") {
			t.Error("expected output to contain error logs retention days '90'")
		}
	})
}

func TestDatabaseBranchSettingsUpdateInvalidInterval(t *testing.T) {
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

		// Test with invalid backup interval (less than 24h)
		err = cli.Run("database", "branch", "settings", "update", fmt.Sprintf("%s/%s", testDatabase.DatabaseName, primaryBranch.Name),
			"--backups-enabled=true",
			"--backup-interval=12h",
			"--backups-retention-days=7",
			"--incremental-backups-enabled=false",
			"--incremental-backups-retention-days=3",
			"--query-logs-enabled=false",
			"--query-logs-retention-days=7",
			"--error-logs-enabled=false",
			"--error-logs-retention-days=7",
		)

		if err == nil {
			t.Error("expected error for invalid backup interval")
		}
	})
}

func TestDatabaseBranchSettingsUpdateInvalidIntervalIncrement(t *testing.T) {
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

		// Test with backup interval that's not a multiple of 24h
		err = cli.Run("database", "branch", "settings", "update", fmt.Sprintf("%s/%s", testDatabase.DatabaseName, primaryBranch.Name),
			"--backups-enabled=true",
			"--backup-interval=36h",
			"--backups-retention-days=7",
			"--incremental-backups-enabled=false",
			"--incremental-backups-retention-days=3",
			"--query-logs-enabled=false",
			"--query-logs-retention-days=7",
			"--error-logs-enabled=false",
			"--error-logs-retention-days=7",
		)

		if err == nil {
			t.Error("expected error for backup interval not in 24h increments")
		}
	})
}

func TestDatabaseBranchSettingsUpdateMissingRequiredInterval(t *testing.T) {
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

		// Test with backups enabled but no interval provided
		err = cli.Run("database", "branch", "settings", "update", fmt.Sprintf("%s/%s", testDatabase.DatabaseName, primaryBranch.Name),
			"--backups-enabled=true",
			"--backup-interval=",
			"--backups-retention-days=7",
			"--incremental-backups-enabled=false",
			"--incremental-backups-retention-days=3",
			"--query-logs-enabled=false",
			"--query-logs-retention-days=7",
			"--error-logs-enabled=false",
			"--error-logs-retention-days=7",
		)

		if err == nil {
			t.Error("expected error when backups enabled but interval not provided")
		}
	})
}

func TestDatabaseBranchSettingsUpdateInvalidPath(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		cli := test.NewTestCLI(t, server.App).
			WithServer(server).
			WithAccessKey([]auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		// Test with invalid database path (no slash)
		err := cli.Run("database", "branch", "settings", "update", "invalid-path",
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

		if err == nil {
			t.Error("expected error for invalid database path")
		}
	})
}

func TestDatabaseBranchSettingsUpdateDisableAll(t *testing.T) {
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

		// First enable everything
		err = cli.Run("database", "branch", "settings", "update", fmt.Sprintf("%s/%s", testDatabase.DatabaseName, primaryBranch.Name),
			"--backups-enabled=true",
			"--backup-interval=24h",
			"--backups-retention-days=7",
			"--incremental-backups-enabled=true",
			"--incremental-backups-retention-days=3",
			"--query-logs-enabled=true",
			"--query-logs-retention-days=7",
			"--error-logs-enabled=true",
			"--error-logs-retention-days=7",
		)

		if err != nil {
			t.Fatalf("expected no error enabling features, got %v", err)
		}

		// Now disable everything
		err = cli.Run("database", "branch", "settings", "update", fmt.Sprintf("%s/%s", testDatabase.DatabaseName, primaryBranch.Name),
			"--backups-enabled=false",
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
			t.Fatalf("expected no error disabling features, got %v", err)
		}

		if cli.DoesNotSee("Branch Settings") {
			t.Error("expected output to contain 'Branch Settings'")
		}
	})
}

func TestDatabaseBranchSettingsUpdateVariousIntervals(t *testing.T) {
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

		// Test various valid intervals
		intervals := []string{"24h", "48h", "72h", "168h"}

		for _, interval := range intervals {
			err = cli.Run("database", "branch", "settings", "update", fmt.Sprintf("%s/%s", testDatabase.DatabaseName, primaryBranch.Name),
				"--backups-enabled=true",
				fmt.Sprintf("--backup-interval=%s", interval),
				"--backups-retention-days=7",
				"--incremental-backups-enabled=false",
				"--incremental-backups-retention-days=3",
				"--query-logs-enabled=false",
				"--query-logs-retention-days=7",
				"--error-logs-enabled=false",
				"--error-logs-retention-days=7",
			)

			if err != nil {
				t.Fatalf("expected no error for interval %s, got %v", interval, err)
			}

			if cli.DoesNotSee(interval) {
				t.Errorf("expected output to contain interval '%s'", interval)
			}
		}
	})
}
