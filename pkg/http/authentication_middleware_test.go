package http_test

import (
	"context"
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

		client := server.WithAccessKeyClient([]auth.Statement{
			{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
		})

		request, err := http.NewRequest("GET", "/users", nil)

		if err != nil {
			t.Fatalf("Failed to create request: %s", err.Error())
		}

		req := appHttp.NewRequest(
			server.App.Cluster,
			server.App.DatabaseManager,
			server.App.LogManager,
			request,
		)

		_, res := appHttp.Authentication(context.Background(), req)

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

		_, res = appHttp.Authentication(context.Background(), req)

		if res.StatusCode != 0 {
			t.Fatalf("Expected status code %d, got %d", 0, res.StatusCode)
		}

		// Test with token authentication
		token, err := server.App.Auth.TokenManager.Create(
			"",
			[]auth.Statement{{
				Effect:   auth.StatementEffectAllow,
				Resource: "*",
			}})

		if err != nil {
			t.Fatal(err)
		}

		tokenValue, err := token.Value()

		if err != nil {
			t.Fatal(err)
		}

		request.Header.Set("Authorization", fmt.Sprintf("Bearer %s", tokenValue))

		req = appHttp.NewRequest(
			server.App.Cluster,
			server.App.DatabaseManager,
			server.App.LogManager,
			request,
		)

		_, res = appHttp.Authentication(context.Background(), req)

		if res.StatusCode != 0 {
			t.Fatalf("Expected status code %d, got %d", 0, res.StatusCode)
		}

		// Test with access key authentication
		signature := auth.SignRequest(
			client.AccessKey.AccessKeyID,
			client.AccessKey.AccessKeySecret,
			"GET",
			"/users",
			map[string]string{
				"Host":            request.URL.Host,
				"Content-Type":    "application/json",
				"X-Litebase-Date": fmt.Sprintf("%d", time.Now().UTC().Unix()),
			},
			nil,
			map[string]string{},
		)

		request.Header.Set("Host", request.URL.Host)
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("X-Litebase-Date", fmt.Sprintf("%d", time.Now().UTC().Unix()))
		request.Header.Set("Authorization", fmt.Sprintf("Litebase-HMAC-SHA256 %s", signature))

		req = appHttp.NewRequest(
			server.App.Cluster,
			server.App.DatabaseManager,
			server.App.LogManager,
			request,
		)

		_, res = appHttp.Authentication(context.Background(), req)

		if res.StatusCode != 0 {
			t.Fatalf("Expected status code %d, got %d", 0, res.StatusCode)
		}
	})
}
