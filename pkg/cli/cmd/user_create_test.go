package cmd_test

import (
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
)

func TestClusterUserCreate(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		cli := test.NewTestCLI(server.App).
			WithServer(server).
			WithAccessKey([]auth.AccessKeyStatement{
				{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		// Test non-interactive mode with flags to avoid TTY issues
		statements := `[{"effect":"allow","resource":"*","actions":["cluster:manage"]}]`
		err := cli.Run("user", "create", "--new-username", "testuser", "--new-password", "testpassword123", "--statements", statements)

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if cli.DoesntSee("User") {
			t.Error("expected output to contain 'User'")
		}

		if cli.DoesntSee("Username") {
			t.Error("expected output to contain 'Username'")
		}

		if cli.Sees("Password") {
			t.Error("expected output to not contain 'Password'")
		}

		if cli.DoesntSee("testuser") {
			t.Error("expected output to contain 'testuser'")
		}

		if cli.DoesntSee("Created At") {
			t.Error("expected output to contain 'Created At'")
		}

		if cli.DoesntSee("Updated At") {
			t.Error("expected output to contain 'Updated At'")
		}

		if cli.DoesntSee("Statements") {
			t.Error("expected output to contain 'Statements'")
		}
	})
}
