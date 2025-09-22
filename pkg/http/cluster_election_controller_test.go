package http_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"
	appHttp "github.com/litebase/litebase/pkg/http"
	"github.com/litebase/litebase/pkg/server"
)

func TestClusterElectionController(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		// Step down the current node to ensure it is not the primary
		if err := app.Cluster.Node().StepDown(); err != nil {
			t.Fatalf("failed to step down node: %v", err)
		}

		// Create a test message to send through the stream
		testMessage := appHttp.ClusterElectionRequest{
			Candidate: server.App.Cluster.Node().ID,
			Seed:      time.Now().UTC().UnixNano(),
			StartedAt: time.Now().UTC().UnixNano(),
		}

		data, err := json.Marshal(testMessage)
		if err != nil {
			t.Fatalf("failed to marshal test message: %v", err)
		}

		request, err := http.NewRequest(
			"POST",
			"/v1/cluster/election",
			bytes.NewBuffer(data),
		)

		if err != nil {
			t.Fatalf("failed to create request: %v", err)
		}

		request.Header.Set("Content-Type", "application/json")

		req := appHttp.NewRequest(
			app.Cluster,
			app.DatabaseManager,
			app.LogManager,
			request,
		)

		res := appHttp.ClusterElectionControllerStore(context.Background(), req)

		if res.StatusCode != http.StatusOK {
			t.Errorf("expected status code 200, got %d", res.StatusCode)
			t.Log("Response Body:", res.Body)
		}
	})
}
