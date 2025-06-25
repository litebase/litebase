package cmd_test

import (
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
)

func TestAccessKeyCreate(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		cli := test.NewTestCLI(server.App).
			WithServer(server).
			WithAccessKey([]auth.AccessKeyStatement{
				{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		// Test non-interactive mode with flags to avoid TTY issues
		statements := `[{"effect":"allow","resource":"*","actions":["*"]}]`
		err := cli.Run("access-key", "create", "--description", "Test access key", "--statements", statements)

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if !cli.ShouldSee("Access Key") {
			t.Error("expected output to contain 'Access Key'")
		}

		if !cli.ShouldSee("Access Key ID") {
			t.Error("expected output to contain 'Access Key ID'")
		}

		if !cli.ShouldSee("Access Key Secret") {
			t.Error("expected output to contain 'Access Key Secrets'")
		}

		if !cli.ShouldSee("Created At") {
			t.Error("expected output to contain 'Created At'")
		}

		if !cli.ShouldSee("Updated At") {
			t.Error("expected output to contain 'Updated At'")
		}

		if !cli.ShouldSee("Statement 1") {
			t.Error("expected output to contain 'Statement 1'")
		}
	})
}
