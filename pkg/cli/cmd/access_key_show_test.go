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

		if !cli.ShouldSee("Access Key") {
			t.Error("expected output to contain 'Access Key'")
		}

		if !cli.ShouldSee("Access Key ID") {
			t.Error("expected output to contain 'Access Key ID'")
		}

		if !cli.ShouldNotSee("Access Key Secret") {
			t.Error("expected output to not contain 'Access Key Secret'")
		}

		if !cli.ShouldSee("Created At") {
			t.Error("expected output to contain 'Created At'")
		}

		if !cli.ShouldSee("Updated At") {
			t.Error("expected output to contain 'Updated At'")
		}

		if !cli.ShouldSee("Statements") {
			t.Errorf("expected output to contain 'Statements' got %q", cli.GetOutput())
		}
	})
}
