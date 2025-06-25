package cmd_test

import (
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
)

func TestStatusCmd(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		cli := test.NewTestCLI(server.App).
			WithServer(server).
			WithAccessKey([]auth.AccessKeyStatement{
				{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		err := cli.Run("status")

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if !cli.ShouldSee("Cluster Status") {
			t.Errorf("expected output to contain 'Cluster Status', got %q", cli.GetOutput())
		}

		if !cli.ShouldSee("Node Count") {
			t.Errorf("expected output to contain 'Node Count', got %q", cli.GetOutput())
		}
	})
}
