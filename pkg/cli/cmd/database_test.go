package cmd_test

import (
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/server"
)

func TestDatabase(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		cli := test.NewTestCLI(app).
			WithAccessKey([]auth.AccessKeyStatement{
				{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		err := cli.Run("database")

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if !cli.Sees("Manage databases") {
			t.Error("expected output to contain 'Manage databases'")
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

		if !cli.Sees("backup") {
			t.Error("expected output to contain 'backup'")
		}

		if !cli.Sees("restore") {
			t.Error("expected output to contain 'restore'")
		}

		if !cli.Sees("update") {
			t.Error("expected output to contain 'update'")
		}

		if !cli.Sees("query") {
			t.Error("expected output to contain 'query'")
		}

		if !cli.Sees("query-logs") {
			t.Error("expected output to contain 'query-logs'")
		}
	})
}
