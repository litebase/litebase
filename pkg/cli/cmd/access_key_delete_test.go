package cmd_test

import (
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
)

func TestAccessKeyDeleteCmd(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		cli := test.NewTestCLI(server.App).
			WithServer(server).
			WithAccessKey([]auth.AccessKeyStatement{
				{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		accessKey, err := server.App.Auth.AccessKeyManager.Create(
			"test",
			[]auth.AccessKeyStatement{
				{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			},
		)

		if err != nil {
			t.Fatalf("failed to create access key: %v", err)
		}

		err = cli.Run("access-key", "delete", accessKey.AccessKeyId)

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if !cli.ShouldSee("Access key deleted") {
			t.Error("expected output to contain 'Access key deleted'")
		}
	})
}
