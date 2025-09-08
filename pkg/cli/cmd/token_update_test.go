package cmd_test

import (
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
)

func TestTokenUpdate(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		cli := test.NewTestCLI(t, server.App).
			WithServer(server).
			WithAccessKey([]auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		token, err := server.App.Auth.TokenManager.Create("Test token", []auth.Statement{
			{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
		})

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		// Test non-interactive mode with flags to avoid TTY issues
		statements := `[{"effect":"deny","resource":"*","actions":["*"]}]`
		err = cli.Run("token", "update", token.TokenID, "--description", "Updated token", "--statements", statements)

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if cli.DoesntSee("Token") {
			t.Error("expected output to contain 'Token'")
		}

		if cli.DoesntSee("Token ID") {
			t.Error("expected output to contain 'Token ID'")
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

		if cli.DoesntSee("Updated token") {
			t.Errorf("expected output to contain 'Updated token', got %q", cli.GetOutput())
		}
	})
}
