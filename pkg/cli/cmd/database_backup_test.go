package cmd_test

import (
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
)

func TestDatabaseBackup(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		cli := test.NewTestCLI(t, server.App).
			WithServer(server).
			WithAccessKey([]auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		err := cli.Run("database", "backup")

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if cli.DoesNotSee("Manage database backups") {
			t.Error("expected output to contain 'Manage database backups'")
		}

		if cli.DoesNotSee("create") {
			t.Error("expected output to contain 'create'")
		}

		if cli.DoesNotSee("delete") {
			t.Error("expected output to contain 'delete'")
		}

		if cli.DoesNotSee("list") {
			t.Error("expected output to contain 'list'")
		}

		if cli.DoesNotSee("show") {
			t.Error("expected output to contain 'show'")
		}
	})
}
