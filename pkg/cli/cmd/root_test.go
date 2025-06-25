package cmd_test

import (
	"testing"

	"github.com/litebase/litebase/internal/test"
)

func TestRootCmd(t *testing.T) {
	test.Run(t, func() {
		cli := test.NewTestCLI(nil)

		err := cli.Run("")
		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if !cli.ShouldSee("Litebase CLI - v") {
			t.Error("expected output to contain 'Litebase CLI - v'")
		}

		if !cli.ShouldSee("Website") {
			t.Error("expected output to contain 'Website'")
		}

		if !cli.ShouldSee("https://litebase.com") {
			t.Error("expected output to contain 'https://litebase.com'")
		}
	})
}
