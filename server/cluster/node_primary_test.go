package cluster_test

import (
	"litebase/internal/test"
	"litebase/server/cluster"
	"litebase/server/cluster/messages"
	"testing"
)

func TestNewNodePrimary(t *testing.T) {
	test.Run(t, func() {
		testServer := test.NewUnstartedTestServer(t)

		node := testServer.App.Cluster.Node()

		primary := cluster.NewNodePrimary(node)

		if primary == nil {
			t.Error("NodePrimary should not be nil")
		}
	})
}

func TestNodePrimaryHeartbeat(t *testing.T) {
	test.Run(t, func() {
		testServer1 := test.NewTestServer(t)
		test.NewTestServer(t)
		test.NewTestServer(t)

		if !testServer1.App.Cluster.Node().IsPrimary() {
			t.Fatalf("Node should be primary")
		}

		err := testServer1.App.Cluster.Node().Primary().Heartbeat()

		if err != nil {
			t.Error("Heartbeat should not return an error")
		}
	})
}

func TestNodePrimaryPublish(t *testing.T) {
	test.Run(t, func() {
		testServer1 := test.NewTestServer(t)
		test.NewTestServer(t)
		test.NewTestServer(t)

		if !testServer1.App.Cluster.Node().IsPrimary() {
			t.Fatalf("Node should be primary")
		}

		err := testServer1.App.Cluster.Node().Primary().Publish(messages.NodeMessage{
			Data: messages.HeartbeatMessage{
				Address: testServer1.App.Cluster.Node().Address(),
				ID:      []byte("broadcast"),
			},
		})

		if err != nil {
			t.Error("Publish should not return an error")
		}
	})
}

func TestNodePrimaryShutdown(t *testing.T) {
	test.Run(t, func() {
		testServer1 := test.NewTestServer(t)
		test.NewTestServer(t)
		test.NewTestServer(t)

		if !testServer1.App.Cluster.Node().IsPrimary() {
			t.Fatalf("Node should be primary")
		}

		testServer1.App.Cluster.Node().Primary().Shutdown()
	})
}
