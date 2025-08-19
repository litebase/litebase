package cmd_test

import (
	"fmt"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
)

func TestTokenListCmd(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		cli := test.NewTestCLI(server.App).
			WithServer(server).
			WithAccessKey([]auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		var tokens []*auth.Token

		for i := range 20 {
			token, err := server.App.Auth.TokenManager.Create(
				fmt.Sprintf("test-%d", i+1),
				[]auth.Statement{
					{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
				},
			)

			if err != nil {
				t.Fatalf("failed to create token: %v", err)
			}

			tokens = append(tokens, token)
		}

		err := cli.Run("token", "list")

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if cli.DoesntSee("#") {
			t.Errorf("expected output to contain '#' got %q", cli.GetOutput())
		}

		if cli.DoesntSee("Token ID") {
			t.Errorf("expected output to contain 'Token ID' got %q", cli.GetOutput())
		}

		for _, token := range tokens {
			if cli.DoesntSee(token.TokenID) {
				t.Errorf("expected output to contain '%s' got %q", token.TokenID, cli.GetOutput())
			}
		}
	})
}
