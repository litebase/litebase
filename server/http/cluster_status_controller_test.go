package http_test

import (
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/server/auth"
)

func TestClusterStatusController(t *testing.T) {
	test.Run(t, func() {
		server1 := test.NewTestServer(t)
		defer server1.Shutdown()

		server2 := test.NewTestServer(t)
		defer server2.Shutdown()

		client := server2.WithAccessKeyClient([]auth.AccessKeyStatement{})

		data, statusCode, err := client.Send("/cluster/status", "GET", nil)

		if err != nil {
			t.Fatalf("Expected no error, got: %v", err)
		}

		if statusCode != 200 {
			t.Fatalf("Expected status code 200, got %d: %v", statusCode, err)
		}

		if data == nil {
			t.Fatal("Expected data to be non-nil")
		}

		if _, ok := data["status"]; !ok {
			t.Fatal("Expected data to contain 'status' key")
		}

		if _, ok := data["data"]; !ok {
			t.Fatal("Expected data to contain 'data' key")
		}

		if _, ok := data["data"].(map[string]any)["region"]; !ok {
			t.Fatal("Expected data['data'] to contain 'region' key")
		}

		if count, ok := data["data"].(map[string]any)["node_count"]; !ok {
			t.Fatal("Expected data['data'] to contain 'node_count' key")
		} else {
			if count.(float64) != 2 {
				t.Fatalf("Expected node_count to be 2, got %f", count)
			}
		}
	})
}
