package http_test

import (
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
)

func TestForwardToPrimary(t *testing.T) {
	test.Run(t, func() {
		server1 := test.NewTestServer(t)
		defer server1.Shutdown()

		server2 := test.NewTestServer(t)
		defer server2.Shutdown()

		client := server2.WithAccessKeyClient([]auth.AccessKeyStatement{
			{
				Effect:   "Allow",
				Resource: "*",
				Actions:  []auth.Privilege{"access-key:create"},
			},
		})

		_, statusCode, err := client.Send("/resources/access-keys", "POST", map[string]any{
			"resource": "*",
			"statements": []map[string]any{
				{
					"effect":   "allow",
					"resource": "*",
					"actions":  []string{"*"},
				},
			},
		})

		if statusCode != 201 {
			t.Fatalf("Expected status code 201, got %d: %v", statusCode, err)
		}

		if err != nil {
			t.Fatalf("Expected no error, got: %v", err)
		}
	})
}
