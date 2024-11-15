package cluster_test

import (
	"fmt"
	"litebase/internal/test"
	"litebase/server/cluster"
	"litebase/server/cluster/messages"
	"strings"
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

func TestNodePrimaryHeartbeatWithDisconnectedNode(t *testing.T) {
	test.Run(t, func() {
		testServer1 := test.NewTestServer(t)
		stoppedAddress := "10.0.0.0:9876"

		err := testServer1.App.Cluster.ObjectFS().WriteFile(
			fmt.Sprintf("%s%s", testServer1.App.Cluster.NodePath(), strings.ReplaceAll(stoppedAddress, ":", "_")),
			[]byte(stoppedAddress),
			0644,
		)

		if err != nil {
			t.Fatalf("Failed to write address file: %v", err)
		}

		testServer1.App.Cluster.GetMembers(false)

		if !testServer1.App.Cluster.Node().IsPrimary() {
			t.Fatalf("Node should be primary")
		}

		err = testServer1.App.Cluster.Node().Primary().Heartbeat()

		if err == nil {
			t.Error("Heartbeat should return an error")
		}

		queryNodes, _ := testServer1.App.Cluster.GetMembers(false)

		if len(queryNodes) != 1 {
			t.Errorf("Query nodes should be 1, got %d", len(queryNodes))
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
