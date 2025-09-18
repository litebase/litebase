package cmd_test

import (
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
)

func TestUserCreate(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		cli := test.NewTestCLI(t, server.App).
			WithServer(server).
			WithAccessKey([]auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		// Test non-interactive mode with flags to avoid TTY issues
		statements := `[{"effect":"allow","resource":"*","actions":["cluster:manage"]}]`
		err := cli.Run("user", "create", "--new-username", "testuser", "--new-password", "testpassword123", "--description", "Test user for CLI testing", "--statements", statements)

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if cli.DoesNotSee("User") {
			t.Error("expected output to contain 'User'")
		}

		if cli.DoesNotSee("Username") {
			t.Error("expected output to contain 'Username'")
		}

		if cli.Sees("Password") {
			t.Error("expected output to not contain 'Password'")
		}

		if cli.DoesNotSee("testuser") {
			t.Error("expected output to contain 'testuser'")
		}

		if cli.DoesNotSee("Created At") {
			t.Error("expected output to contain 'Created At'")
		}

		if cli.DoesNotSee("Updated At") {
			t.Error("expected output to contain 'Updated At'")
		}

		if cli.DoesNotSee("Statements") {
			t.Error("expected output to contain 'Statements'")
		}
	})

	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		cli := test.NewTestCLI(t, server.App).
			WithServer(server).
			WithAccessKey([]auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		// Test without description (optional field)
		statements := `[{"effect":"allow","resource":"*","actions":["cluster:manage"]}]`
		err := cli.Run("user", "create", "--new-username", "testuser2", "--new-password", "testpassword123", "--statements", statements)

		if err != nil {
			t.Fatalf("expected no error when creating user without description, got %v", err)
		}

		if cli.DoesNotSee("User") {
			t.Error("expected output to contain 'User'")
		}

		if cli.DoesNotSee("testuser2") {
			t.Error("expected output to contain 'testuser2'")
		}
	})
}
