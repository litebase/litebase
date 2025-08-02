package http_test

import (
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
)

func TestKeyActivateController(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		nextKey := test.CreateHash(64)

		err := auth.NextEncryptionKey(
			server.App.Auth,
			server.App.Config,
			nextKey,
		)

		if err != nil {
			t.Fatalf("Failed to store encryption key: %v", err)
		}

		client := server.WithAccessKeyClient([]auth.AccessKeyStatement{
			{
				Effect:   "Allow",
				Resource: "*",
				Actions:  []auth.Privilege{auth.ClusterPrivilegeManage},
			},
		})

		response, statusCode, err := client.Send("/v1/keys/activate", "POST", map[string]any{
			"encryption_key": nextKey,
		})

		if err != nil {
			t.Fatalf("Failed to activate encryption key: %v", err)
		}

		if statusCode != 200 {
			t.Log("Response:", response["message"])
			t.Fatalf("Unexpected status code: %d, expected 200", statusCode)
		}

		if response["status"] != "success" {
			t.Errorf("Unexpected response: %v", response)
		}

		if response["message"] != "encryption key activated successfully" {
			t.Errorf("Unexpected message: %s, expected 'encryption key activated successfully'", response["message"])
		}
	})
}
