package http_test

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
	appHttp "github.com/litebase/litebase/pkg/http"
)

func TestAuthenticationMiddleware(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		client := server.WithAccessKeyClient([]auth.AccessKeyStatement{
			{Effect: "Allow", Resource: "*", Actions: []auth.Privilege{"*"}},
		})

		request, err := http.NewRequest("GET", "/resources/users", nil)

		if err != nil {
			t.Fatalf("Failed to create request: %s", err.Error())
		}

		req := appHttp.NewRequest(
			server.App.Cluster,
			server.App.DatabaseManager,
			server.App.LogManager,
			request,
		)

		_, res := appHttp.Authentication(req)

		if res.StatusCode != http.StatusUnauthorized {
			t.Fatalf("Expected status code %d, got %d", http.StatusUnauthorized, res.StatusCode)
		}

		// Test with basic authentication
		request.SetBasicAuth(server.App.Config.RootUsername, server.App.Config.RootPassword)

		req = appHttp.NewRequest(
			server.App.Cluster,
			server.App.DatabaseManager,
			server.App.LogManager,
			request,
		)

		_, res = appHttp.Authentication(req)

		if res.StatusCode != 0 {
			t.Fatalf("Expected status code %d, got %d", 0, res.StatusCode)
		}

		// Test with access key authentication
		signature := auth.SignRequest(
			client.AccessKey.AccessKeyId,
			client.AccessKey.AccessKeySecret,
			"GET",
			"/resources/users",
			map[string]string{
				"Host":         request.URL.Host,
				"Content-Type": "application/json",
				"X-LBDB-Date":  fmt.Sprintf("%d", time.Now().UTC().Unix()),
			},
			nil,
			map[string]string{},
		)

		request.Header.Set("Host", request.URL.Host)
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("X-LBDB-Date", fmt.Sprintf("%d", time.Now().UTC().Unix()))
		request.Header.Set("Authorization", signature)

		req = appHttp.NewRequest(
			server.App.Cluster,
			server.App.DatabaseManager,
			server.App.LogManager,
			request,
		)

		_, res = appHttp.Authentication(req)

		if res.StatusCode != 0 {
			t.Fatalf("Expected status code %d, got %d", 0, res.StatusCode)
		}
	})
}
