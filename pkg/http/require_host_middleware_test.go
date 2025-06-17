package http_test

import (
	"net/http"
	"testing"

	"github.com/litebase/litebase/internal/test"
	appHttp "github.com/litebase/litebase/pkg/http"
)

func TestRequireHostMiddleware(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		request, err := http.NewRequest("GET", "http://localhost/resources/users", nil)

		if err != nil {
			t.Fatalf("Failed to create request: %s", err.Error())
		}

		req := appHttp.NewRequest(
			server.App.Cluster,
			server.App.DatabaseManager,
			server.App.LogManager,
			request,
		)

		_, res := appHttp.RequireHost(req)

		if res.StatusCode != 0 {
			t.Fatalf("Expected status code %d, got %d", 0, res.StatusCode)
		}

		request, err = http.NewRequest("GET", "/resources/users", nil)

		if err != nil {
			t.Fatalf("Failed to create request: %s", err.Error())
		}

		req = appHttp.NewRequest(
			server.App.Cluster,
			server.App.DatabaseManager,
			server.App.LogManager,
			request,
		)

		_, res = appHttp.RequireHost(req)

		if res.StatusCode != 400 {
			t.Fatalf("Expected status code %d, got %d", 400, res.StatusCode)
		}

		request, err = http.NewRequest("GET", "http://foo.dev/resources/users", nil)

		if err != nil {
			t.Fatalf("Failed to create request: %s", err.Error())
		}

		req = appHttp.NewRequest(
			server.App.Cluster,
			server.App.DatabaseManager,
			server.App.LogManager,
			request,
		)

		_, res = appHttp.RequireHost(req)

		if res.StatusCode != 403 {
			t.Fatalf("Expected status code %d, got %d", 403, res.StatusCode)
		}

		request, err = http.NewRequest("GET", "http://127.0.0.1:1234/resources/users", nil)

		if err != nil {
			t.Fatalf("Failed to create request: %s", err.Error())
		}

		req = appHttp.NewRequest(
			server.App.Cluster,
			server.App.DatabaseManager,
			server.App.LogManager,
			request,
		)

		_, res = appHttp.RequireHost(req)

		if res.StatusCode != 0 {
			t.Fatalf("Expected status code %d, got %d", 0, res.StatusCode)
		}

		request, err = http.NewRequest("GET", "http://192.168.0.1:1234/resources/users", nil)

		if err != nil {
			t.Fatalf("Failed to create request: %s", err.Error())
		}

		req = appHttp.NewRequest(
			server.App.Cluster,
			server.App.DatabaseManager,
			server.App.LogManager,
			request,
		)

		_, res = appHttp.RequireHost(req)

		if res.StatusCode != 403 {
			t.Fatalf("Expected status code %d, got %d", 403, res.StatusCode)
		}
	})
}
