package http_test

import (
	"net/http"
	"testing"

	"github.com/litebase/litebase/internal/test"
	appHttp "github.com/litebase/litebase/pkg/http"
	"github.com/litebase/litebase/pkg/server"
)

func TestNodeTickMiddleware(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		request, err := http.NewRequest("GET", "/resources/users", nil)

		lastActive := app.Cluster.Node().LastActive

		if err != nil {
			t.Fatalf("Failed to create request: %s", err.Error())
		}

		req := appHttp.NewRequest(
			app.Cluster,
			app.DatabaseManager,
			app.LogManager,
			request,
		)

		_, res := appHttp.NodeTick(req)

		if res.StatusCode != 0 {
			t.Fatalf("Expected status code %d, got %d", 0, res.StatusCode)
		}

		if app.Cluster.Node().LastActive.Before(lastActive) {
			t.Fatalf("Expected last active to be updated")
		}

		if app.Cluster.Node().LastActive.Equal(lastActive) {
			t.Fatalf("Expected last active to be updated")
		}
	})
}
