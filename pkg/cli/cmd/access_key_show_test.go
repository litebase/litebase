package cmd_test

import (
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
)

func TestAccessKeyShow(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		cli := test.NewTestCLI(server.App).
			WithServer(server).
			WithAccessKey([]auth.AccessKeyStatement{
				{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		err := cli.Run("access-key", "show", cli.AccessKey.AccessKeyId)

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if cli.DoesntSee("Access Key") {
			t.Error("expected output to contain 'Access Key'")
		}

		if cli.DoesntSee("Access Key ID") {
			t.Error("expected output to contain 'Access Key ID'")
		}

		if cli.Sees("Access Key Secret") {
			t.Error("expected output to not contain 'Access Key Secret'")
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
