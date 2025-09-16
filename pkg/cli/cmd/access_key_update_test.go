package cmd_test

import (
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
)

func TestAccessKeyUpdate(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		cli := test.NewTestCLI(t, server.App).
			WithServer(server).
			WithAccessKey([]auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		accessKey, err := server.App.Auth.AccessKeyManager.Create("Test access key", []auth.Statement{
			{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
		})

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		// Test non-interactive mode with flags to avoid TTY issues
		statements := `[{"effect":"deny","resource":"*","actions":["*"]}]`
		err = cli.Run("access-key", "update", accessKey.AccessKeyID, "--description", "Updated access key", "--statements", statements)

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if cli.DoesNotSee("Access Key") {
			t.Error("expected output to contain 'Access Key'")
		}

		if cli.DoesNotSee("Access Key ID") {
			t.Error("expected output to contain 'Access Key ID'")
		}

		if cli.Sees("Access Key Secret") {
			t.Error("expected output to not contain 'Access Key Secret'")
		}

		if cli.DoesNotSee("Created At") {
			t.Error("expected output to contain 'Created At'")
		}

		if cli.DoesNotSee("Updated At") {
			t.Error("expected output to contain 'Updated At'")
		}

		if cli.DoesNotSee("Statements") {
			t.Error("expected output to contain 'Statements'")
		}

		if cli.DoesNotSee("deny") {
			t.Error("expected output to contain 'deny'")
		}

		if cli.DoesNotSee("Updated access key") {
			t.Errorf("expected output to contain 'Updated access key', got %q", cli.GetOutput())
		}
	})
}
