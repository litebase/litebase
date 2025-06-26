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

		cli := test.NewTestCLI(server.App).
			WithServer(server).
			WithAccessKey([]auth.AccessKeyStatement{
				{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		user, err := server.App.Auth.UserManager().Add("Test user", "password", []auth.AccessKeyStatement{
			{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
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

		if cli.DoesntSee("User") {
			t.Error("expected output to contain 'User'")
		}

		if cli.DoesntSee("User Name") {
			t.Error("expected output to contain 'User Name'")
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

		if cli.DoesntSee("deny") {
			t.Error("expected output to contain 'deny'")
		}

		if cli.DoesntSee(fmt.Sprintf("User '%s' updated successfully", user.Username)) {
			t.Errorf("expected output to contain 'User '%s' updated successfully', got %q", user.Username, cli.GetOutput())
		}
	})
}
