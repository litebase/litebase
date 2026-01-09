package cmd_test

import (
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
)

func TestDatabaseBranchSettingsCommand(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		cli := test.NewTestCLI(t, server.App).
			WithServer(server).
			WithAccessKey([]auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		// Test that the settings command exists and shows help
		err := cli.Run("database", "branch", "settings", "--help")

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if cli.DoesNotSee("Manage database branch settings") {
			t.Error("expected output to contain command description")
		}

		if cli.DoesNotSee("show") {
			t.Error("expected output to contain 'show' subcommand")
		}

		if cli.DoesNotSee("update") {
			t.Error("expected output to contain 'update' subcommand")
		}
	})
}

func TestDatabaseBranchSettingsCommandRequiresSubcommand(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		cli := test.NewTestCLI(t, server.App).
			WithServer(server).
			WithAccessKey([]auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		// Test that running settings without a subcommand shows help
		err := cli.Run("database", "branch", "settings")

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		// Should show help when no subcommand provided
		if cli.DoesNotSee("Manage database branch settings") {
			t.Error("expected output to contain command description")
		}
	})
}
