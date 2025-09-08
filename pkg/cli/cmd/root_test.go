package cmd_test

import (
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/server"
)

func TestRootCmd(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		cli := test.NewTestCLI(t, app)

		err := cli.Run("")
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if cli.DoesntSee("Litebase CLI - test") {
			t.Error("expected output to contain 'Litebase CLI - test'")
		}

		if cli.DoesntSee("Website") {
			t.Error("expected output to contain 'Website'")
		}

		if cli.DoesntSee("https://litebase.com") {
			t.Error("expected output to contain 'https://litebase.com'")
		}
	})
}
