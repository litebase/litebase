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

		cli := test.NewTestCLI(server.App).
			WithServer(server).
			WithAccessKey([]auth.AccessKeyStatement{
				{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		user, err := server.App.Auth.UserManager().Add(
			"testuser",
			"testpassword123",
			[]auth.AccessKeyStatement{
				{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			},
		)

		if err != nil {
			t.Fatalf("failed to create user: %v", err)
		}

		err = cli.Run("user", "show", user.Username)

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if cli.DoesntSee("User") {
			t.Error("expected output to contain 'User'")
		}

		if cli.DoesntSee("User Name") {
			t.Error("expected output to contain 'User Name'")
		}

		if cli.Sees("Password") {
			t.Error("expected output to not contain 'Password'")
		}

		if cli.DoesntSee("Created At") {
			t.Error("expected output to contain 'Created At'")
		}

		if cli.DoesntSee("Updated At") {
			t.Error("expected output to contain 'Updated At'")
		}

		if cli.DoesntSee("Statements") {
			t.Errorf("expected output to contain 'Statements' got %q", cli.GetOutput())
		}
	})
}
