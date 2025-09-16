package cmd_test

import (
	"fmt"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
)

func TestUserUpdate(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		cli := test.NewTestCLI(t, server.App).
			WithServer(server).
			WithAccessKey([]auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		user, err := server.App.Auth.UserManager.Create("Test user", "password", "", []auth.Statement{
			{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
		})

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		// Test non-interactive mode with flags to avoid TTY issues
		statements := `[{"effect":"deny","resource":"*","actions":["*"]}]`
		err = cli.Run("user", "update", user.Username, "--statements", statements)

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if cli.DoesNotSee("User") {
			t.Error("expected output to contain 'User'")
		}

		if cli.DoesNotSee("User Name") {
			t.Error("expected output to contain 'User Name'")
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

		if cli.DoesNotSee("deny") {
			t.Error("expected output to contain 'deny'")
		}

		if cli.DoesNotSee(fmt.Sprintf("User '%s' updated successfully", user.Username)) {
			t.Errorf("expected output to contain 'User '%s' updated successfully', got %q", user.Username, cli.GetOutput())
		}
	})
}
