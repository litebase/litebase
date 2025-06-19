package cluster_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/cluster"
	"github.com/litebase/litebase/pkg/cluster/messages"
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
		defer testServer1.Shutdown()
		testServer2 := test.NewTestServer(t)
		defer testServer2.Shutdown()
		testServer3 := test.NewTestServer(t)
		defer testServer3.Shutdown()

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
		defer testServer1.Shutdown()

		stoppedAddress := "10.0.0.0:9876"

		err := testServer1.App.Cluster.NetworkFS().WriteFile(
			fmt.Sprintf("%s%s", testServer1.App.Cluster.NodePath(), strings.ReplaceAll(stoppedAddress, ":", "_")),
			[]byte(stoppedAddress),
			0600,
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

		queryNodes := testServer1.App.Cluster.GetMembers(false)

		if len(queryNodes) != 1 {
			t.Errorf("Query nodes should be 1, got %d", len(queryNodes))
		}
	})
}

func TestNodePrimaryPublish(t *testing.T) {
	test.Run(t, func() {
		testServer1 := test.NewTestServer(t)
		defer testServer1.Shutdown()
		testServer2 := test.NewTestServer(t)
		defer testServer2.Shutdown()
		testServer3 := test.NewTestServer(t)
		defer testServer3.Shutdown()
		address, _ := testServer1.App.Cluster.Node().Address()

		if !testServer1.App.Cluster.Node().IsPrimary() {
			t.Fatalf("Node should be primary")
		}

		_, err := testServer1.App.Cluster.Node().Primary().Publish(messages.NodeMessage{
			Data: messages.HeartbeatMessage{
				Address: address,
				ID:      []byte("broadcast"),
			},
		})

		if len(err) > 0 {
			t.Errorf("Publish should not return an error: %v", err)
		}
	})
}

func TestNodePrimaryShutdown(t *testing.T) {
	test.Run(t, func() {
		testServer1 := test.NewTestServer(t)
		defer testServer1.Shutdown()
		testServer2 := test.NewTestServer(t)
		defer testServer2.Shutdown()
		testServer3 := test.NewTestServer(t)
		defer testServer3.Shutdown()

		if !testServer1.App.Cluster.Node().IsPrimary() {
			t.Fatalf("Node should be primary")
		}

		testServer1.App.Cluster.Node().Primary().Shutdown()
	})
}
