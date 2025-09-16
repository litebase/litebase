package cmd_test

import (
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
)

func TestTokenShow(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		cli := test.NewTestCLI(t, server.App).
			WithServer(server).
			WithAccessKey([]auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		token, err := server.App.Auth.TokenManager.Create(
			"test-token",
			[]auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			},
		)

		if err != nil {
			t.Fatalf("failed to create token: %v", err)
		}

		err = cli.Run("token", "show", token.TokenID)

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if cli.DoesNotSee("Token") {
			t.Error("expected output to contain 'Token'")
		}

		if cli.DoesNotSee("Token ID") {
			t.Error("expected output to contain 'Token ID'")
		}

		if cli.DoesNotSee("Created At") {
			t.Error("expected output to contain 'Created At'")
		}

		if cli.DoesNotSee("Updated At") {
			t.Error("expected output to contain 'Updated At'")
		}

		if cli.DoesNotSee("Statements") {
			t.Errorf("expected output to contain 'Statements' got %q", cli.GetOutput())
		}
	})
}
