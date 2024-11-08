package cluster_test

import (
	"litebase/internal/test"
	"litebase/server"
	"litebase/server/cluster"
	"litebase/server/cluster/messages"
	"testing"
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

		connection := cluster.NewNodeConnection(server1.App.Cluster.Node(), server2.App.Cluster.Node().Address())

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

		connection := cluster.NewNodeConnection(server1.App.Cluster.Node(), server2.App.Cluster.Node().Address())

		_, err := connection.Send(messages.NodeMessage{
			Data: "hello",
		})

		if err == nil {
			t.Errorf("expected error to be not nil")
		}
	})
}
