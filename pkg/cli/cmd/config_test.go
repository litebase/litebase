package cmd_test

import (
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/server"
)

func TestConfigCmd(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		cli := test.NewTestCLI(t, app).
			WithAccessKey([]auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		err := cli.Run("config")

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if !cli.Sees("Manage Litebase Server configuration") {
			t.Error("expected output to contain 'Manage Litebase Server configuration'")
		}

		if !cli.Sees("show") {
			t.Error("expected output to contain 'show'")
		}

		if !cli.Sees("init") {
			t.Error("expected output to contain 'init'")
		}
	})
}
