package http_test

import (
	"crypto/hmac"
	"crypto/sha256"
	"fmt"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
)

func TestKeyControllerStore(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		client := server.WithAccessKeyClient([]auth.AccessKeyStatement{
			{
				Effect:   "Allow",
				Resource: "*",
				Actions:  []auth.Privilege{auth.ClusterPrivilegeManage},
			},
		})

		encryptionKey := test.CreateHash(32)
		hash := hmac.New(sha256.New, []byte(server.App.Config.EncryptionKey))
		hash.Write([]byte(encryptionKey))
		hmacHexSignature := fmt.Sprintf("%x", hash.Sum(nil))

		response, statusCode, err := client.Send("/v1/keys", "POST", map[string]any{
			"encryption_key": encryptionKey,
			"signature":      hmacHexSignature,
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

		if response["message"] != "next encryption key stored successfully" {
			t.Errorf("Unexpected message: %s, expected 'next encryption key stored successfully'", response["message"])
		}
	})
}
