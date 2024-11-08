package cluster_test

import (
	"litebase/internal/test"
	"litebase/server"
	"litebase/server/cluster/messages"
	"testing"
	"time"
)

func TestHandleMessage(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		// Test a known message type.
		resp, err := app.Cluster.Node().HandleMessage(messages.NodeMessage{
			Data: messages.HeartbeatMessage{
				Time: time.Now().Unix(),
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
