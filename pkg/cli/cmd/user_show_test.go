package cmd_test

import (
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
)

func TestUserShow(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		cli := test.NewTestCLI(t, server.App).
			WithServer(server).
			WithAccessKey([]auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		user, err := server.App.Auth.UserManager.Create(
			"testuser",
			"testpassword123",
			"",
			[]auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			},
		)

		if err != nil {
			t.Fatalf("failed to create user: %v", err)
		}

		err = cli.Run("user", "show", user.Username)

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if cli.DoesNotSee("User") {
			t.Error("expected output to contain 'User'")
		}

		if cli.DoesNotSee("User Name") {
			t.Error("expected output to contain 'User Name'")
		}

		if cli.Sees("Password") {
			t.Error("expected output to not contain 'Password'")
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
