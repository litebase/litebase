package http_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/litebase/litebase/internal/test"
	appHttp "github.com/litebase/litebase/pkg/http"
	"github.com/litebase/litebase/pkg/server"
)

func TestPreloadDatabaseKey(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		request, err := http.NewRequest("GET", fmt.Sprintf("/%s", mock.DatabaseKey.Key), nil)

		if err != nil {
			t.Fatalf("Failed to create request: %s", err.Error())
		}

		request.SetPathValue("databaseKey", mock.DatabaseKey.Key)

		req := appHttp.NewRequest(
			app.Cluster,
			app.DatabaseManager,
			app.LogManager,
			request,
		)

		_, res := appHttp.PreloadDatabaseKey(req)

		if res.StatusCode != 0 {
			t.Fatalf("Expected status code %d, got %d", 0, res.StatusCode)
		}
	})
}
