package cluster_test

import (
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/server"
	"github.com/litebase/litebase/server/cluster/messages"
)

func TestHandleMessage(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		// Test a known message type.
		resp, err := app.Cluster.Node().HandleMessage(messages.NodeMessage{
			Data: messages.HeartbeatMessage{
				Time: time.Now().UTC().Unix(),
			},
		})

		if err != nil {
			t.Error(err)
		}

		if _, ok := resp.Data.(messages.ErrorMessage); ok {
			t.Error("Expected heartbeat response")
		}

		// Test an unknown message type.
		resp, err = app.Cluster.Node().HandleMessage(messages.NodeMessage{
			Data: "unknown message type",
		})

		if err != nil {
			t.Error(err)
		}

		if _, ok := resp.Data.(messages.ErrorMessage); !ok {
			t.Error("Expected error response")
		}
	})
}
