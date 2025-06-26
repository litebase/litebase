package cmd_test

import (
	"fmt"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
)

func TestUserListCmd(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		cli := test.NewTestCLI(server.App).
			WithServer(server).
			WithAccessKey([]auth.AccessKeyStatement{
				{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		var users []*auth.User

		for i := range 20 {
			user, err := server.App.Auth.UserManager().Add(
				fmt.Sprintf("testuser-%d", i+1),
				"testpassword123",
				[]auth.AccessKeyStatement{
					{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
				},
			)

			if err != nil {
				t.Fatalf("failed to create user: %v", err)
			}

			users = append(users, user)
		}

		err := cli.Run("user", "list")

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if cli.DoesntSee("#") {
			t.Errorf("expected output to contain '#' got %q", cli.GetOutput())
		}

		if cli.DoesntSee("Username") {
			t.Errorf("expected output to contain 'Username' got %q", cli.GetOutput())
		}

		for _, user := range users {
			if cli.DoesntSee(user.Username) {
				t.Errorf("expected output to contain '%s' got %q", user.Username, cli.GetOutput())
			}
		}
	})
}
