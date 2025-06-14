package http_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/server"
	appHttp "github.com/litebase/litebase/server/http"
)

func TestClusterElectionController(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		server2 := test.NewTestServer(t)

		// Step down the current node to ensure it is not the primary
		app.Cluster.Node().StepDown()

		// Create a test message to send through the stream
		testMessage := appHttp.ClusterElectionRequest{
			Candidate: server2.App.Cluster.Node().ID,
			Seed:      time.Now().UTC().UnixNano(),
			StartedAt: time.Now().UTC().UnixNano(),
		}

		data, err := json.Marshal(testMessage)
		if err != nil {
			t.Fatalf("failed to marshal test message: %v", err)
		}

		request, err := http.NewRequest(
			"POST",
			"/cluster/election",
			bytes.NewBuffer(data),
		)

		if err != nil {
			t.Fatalf("failed to create request: %v", err)
		}

		req := appHttp.NewRequest(
			app.Cluster,
			app.DatabaseManager,
			app.LogManager,
			request,
		)

		res := appHttp.ClusterElectionController(req)

		if res.StatusCode != http.StatusOK {
			t.Errorf("expected status code 200, got %d", res.StatusCode)
			t.Log("Response Body:", res.Body)
		}
	})
}
