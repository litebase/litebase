package http_test

import (
	"net/http"
	"testing"

	"github.com/litebase/litebase/internal/test"
	appHttp "github.com/litebase/litebase/pkg/http"
)

func TestRequireContentTypeMiddleware(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		request, err := http.NewRequest("GET", "http://localhost/resources/users", nil)

		if err != nil {
			t.Fatalf("Failed to create request: %s", err.Error())
		}

		request.Header.Set("Content-Type", "application/json")

		req := appHttp.NewRequest(
			server.App.Cluster,
			server.App.DatabaseManager,
			server.App.LogManager,
			request,
		)

		_, res := appHttp.RequireContentType(req)

		if res.StatusCode != 0 {
			t.Fatalf("Expected status code %d, got %d", 0, res.StatusCode)
		}

		request, err = http.NewRequest("POST", "/resources/users", nil)

		if err != nil {
			t.Fatalf("Failed to create request: %s", err.Error())
		}

		req = appHttp.NewRequest(
			server.App.Cluster,
			server.App.DatabaseManager,
			server.App.LogManager,
			request,
		)

		_, res = appHttp.RequireContentType(req)

		if res.StatusCode != 400 {
			t.Fatalf("Expected status code %d, got %d", 400, res.StatusCode)
		}

		request, err = http.NewRequest("POST", "http://localhost/resources/users", nil)
		if err != nil {
			t.Fatalf("Failed to create request: %s", err.Error())
		}

		request.Header.Set("Content-Type", "image/svg")

		req = appHttp.NewRequest(
			server.App.Cluster,
			server.App.DatabaseManager,
			server.App.LogManager,
			request,
		)

		_, res = appHttp.RequireContentType(req)

		if res.StatusCode != 415 {
			t.Fatalf("Expected status code %d, got %d", 415, res.StatusCode)
		}
	})
}
