package cmd_test

import (
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
)

func TestDatabaseCreate(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		cli := test.NewTestCLI(server.App).
			WithServer(server).
			WithAccessKey([]auth.AccessKeyStatement{
				{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		err := cli.Run("database", "create", "test")

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if cli.DoesntSee("Database") {
			t.Error("expected output to contain 'Database'")
		}

		if cli.DoesntSee("Name") {
			t.Error("expected output to contain 'Name'")
		}

		if cli.DoesntSee("Created At") {
			t.Error("expected output to contain 'Created At'")
		}

		if cli.DoesntSee("Updated At") {
			t.Error("expected output to contain 'Updated At'")
		}

		if cli.DoesntSee("URL") {
			t.Error("expected output to contain 'URL'")
		}
	})
}

func TestDatabaseCreateWithPrimaryBranch(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		cli := test.NewTestCLI(server.App).
			WithServer(server).
			WithAccessKey([]auth.AccessKeyStatement{
				{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		err := cli.Run("database", "create", "test", "--primary-branch", "primary")

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if cli.DoesntSee("Database") {
			t.Error("expected output to contain 'Database'")
		}

		db, err := server.App.DatabaseManager.GetByName("test")

		if err != nil {
			t.Fatalf("expected to get database, got error: %v", err)
		}

		if db.PrimaryBranch().Name != "primary" {
			t.Errorf("expected primary branch to be 'primary', got '%s'", db.PrimaryBranch().Name)
		}
	})
}
