package cluster_test

import (
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/cluster"
	"github.com/litebase/litebase/pkg/cluster/messages"
	"github.com/litebase/litebase/server"
)

func TestNewNodeConnection(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		connection := cluster.NewNodeConnection(app.Cluster.Node(), "localhost:8080")

		if connection == nil {
			t.Fatalf("expected connection to be not nil")
		}

		if connection.Address != "localhost:8080" {
			t.Errorf("expected address to be localhost:8080, got %s", connection.Address)
		}
	})
}

func TestNodeConnectionClose(t *testing.T) {
	test.Run(t, func() {
		server1 := test.NewTestServer(t)
		server2 := test.NewTestServer(t)
		address2, _ := server2.App.Cluster.Node().Address()
		connection := cluster.NewNodeConnection(server1.App.Cluster.Node(), address2)

		err := connection.Close()

		if err != nil {
			t.Errorf("expected error to be nil, got %v", err)
		}
	})
}

func TestNodeConnectionSend(t *testing.T) {
	test.Run(t, func() {
		server1 := test.NewTestServer(t)
		server2 := test.NewTestServer(t)
		address2, _ := server2.App.Cluster.Node().Address()

		connection := cluster.NewNodeConnection(server1.App.Cluster.Node(), address2)

		_, err := connection.Send(messages.NodeMessage{
			Data: "hello",
		})

		if err == nil {
			t.Errorf("expected error to be not nil")
		}
	})
}
