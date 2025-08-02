package cmd_test

import (
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/server"
)

func TestAccessKey(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		cli := test.NewTestCLI(app).
			WithAccessKey([]auth.AccessKeyStatement{
				{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		err := cli.Run("access-key")

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if !cli.Sees("Manage access keys") {
			t.Error("expected output to contain 'Manage access keys'")
		}

		if !cli.Sees("create") {
			t.Error("expected output to contain 'create'")
		}

		if !cli.Sees("delete") {
			t.Error("expected output to contain 'delete'")
		}

		if !cli.Sees("list") {
			t.Error("expected output to contain 'list'")
		}

		if !cli.Sees("show") {
			t.Error("expected output to contain 'show'")
		}

		if !cli.Sees("update") {
			t.Error("expected output to contain 'update'")
		}
	})
}
