package cmd_test

import (
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
)

func TestTokenDeleteCmd(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		cli := test.NewTestCLI(server.App).
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

		err = cli.Run("token", "delete", token.TokenID)

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if cli.DoesntSee("Token deleted") {
			t.Error("expected output to contain 'Token deleted'")
		}
	})
}
