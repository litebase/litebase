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

		// Verify cluster setup
		if !server1.App.Cluster.Node().IsPrimary() {
			t.Fatal("Server1 should be primary")
		}

		if !server2.App.Cluster.Node().IsReplica() {
			t.Fatal("Server2 should be replica")
		}

		accessKey, err := server1.App.Auth.AccessKeyManager.Create("Test access key", []auth.Statement{
			{
				Effect:   auth.StatementEffectAllow,
				Resource: "*",
				Actions:  []auth.Privilege{"access-key:create"},
			},
		})

		if err != nil {
			t.Error("Expected Create to return no error")
		}

		client := server2.WithAccessKey(accessKey)

		_, statusCode, err := client.Send("/v1/access-keys", "POST", map[string]any{
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
