package cmd_test

import (
	"fmt"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
)

func TestAccessKeyListCmd(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		cli := test.NewTestCLI(server.App).
			WithServer(server).
			WithAccessKey([]auth.AccessKeyStatement{
				{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

		var accessKeys []*auth.AccessKey

		for i := range 20 {
			accessKey, err := server.App.Auth.AccessKeyManager.Create(
				fmt.Sprintf("test-%d", i+1),
				[]auth.AccessKeyStatement{
					{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
				},
			)

			if err != nil {
				t.Fatalf("failed to create access key: %v", err)
			}

			accessKeys = append(accessKeys, accessKey)
		}

		err := cli.Run("access-key", "list")

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if !cli.ShouldSee("#") {
			t.Errorf("expected output to contain '#' got %q", cli.GetOutput())
		}

		if !cli.ShouldSee("Access Key ID") {
			t.Errorf("expected output to contain 'Access Key ID' got %q", cli.GetOutput())
		}

		for _, accessKey := range accessKeys {
			if !cli.ShouldSee(accessKey.AccessKeyId) {
				t.Errorf("expected output to contain '%s' got %q", accessKey.AccessKeyId, cli.GetOutput())
			}
		}
	})
}
