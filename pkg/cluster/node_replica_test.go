package cluster_test

import (
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/cluster"
	"github.com/litebase/litebase/pkg/cluster/messages"
)

func TestNewNodeReplica(t *testing.T) {
	test.Run(t, func() {
		testServer := test.NewUnstartedTestServer(t)

		node := testServer.App.Cluster.Node()

		replica := cluster.NewNodeReplica(node)

		if replica == nil {
			t.Error("NodeReplica should not be nil")
		}
	})
}

func TestNodeReplicaJoinCluster(t *testing.T) {
	test.Run(t, func() {
		testServer1 := test.NewTestServer(t)
		defer testServer1.Shutdown()

		if !testServer1.App.Cluster.Node().IsPrimary() {
			t.Fatalf("Node should be primary")
		}

		queryNodes := testServer1.App.Cluster.GetMembers(true)

		if len(queryNodes) != 1 {
			t.Errorf("Expected 1 nodes, got %d", len(queryNodes))
		}

		testServer2 := test.NewUnstartedTestServer(t)

		testServer2.App.Cluster.Node().SetMembership(cluster.ClusterMembershipReplica)
		err := testServer2.App.Cluster.Node().StoreAddress()

		if err != nil {
			t.Fatalf("StoreAddress should not return an error")
		}

		if !testServer2.App.Cluster.Node().IsReplica() {
			t.Fatalf("Node should be replica")
		}

		err = testServer2.App.Cluster.Node().Replica().JoinCluster()

		if err != nil {
			t.Error("JoinCluster should not return an error")
		}

		queryNodes = testServer1.App.Cluster.GetMembers(true)

		if len(queryNodes) != 2 {
			t.Errorf("Expected 2 nodes, got %d", len(queryNodes))
		}
	})
}

func TestNodeReplicaLeaveCluster(t *testing.T) {
	test.Run(t, func() {
		testServer1 := test.NewTestServer(t)
		defer testServer1.Shutdown()

		if !testServer1.App.Cluster.Node().IsPrimary() {
			t.Fatalf("Node should be primary")
		}

		queryNodes := testServer1.App.Cluster.GetMembers(true)

		if len(queryNodes) != 1 {
			t.Errorf("Expected 1 nodes, got %d", len(queryNodes))
		}

		testServer2 := test.NewTestServer(t)
		defer testServer2.Shutdown()

		queryNodes = testServer1.App.Cluster.GetMembers(true)

		if len(queryNodes) != 2 {
			t.Errorf("Expected 2 nodes, got %d", len(queryNodes))
		}

		err := testServer2.App.Cluster.Node().Replica().LeaveCluster()

		if err != nil {
			t.Error("LeaveCluster should not return an error")
		}

		queryNodes = testServer1.App.Cluster.GetMembers(true)

		if len(queryNodes) != 1 {
			t.Errorf("Expected 1 nodes, got %d", len(queryNodes))
		}
	})
}

func TestNodeReplicaSend(t *testing.T) {
	test.Run(t, func() {
		testServer1 := test.NewTestServer(t)
		defer testServer1.Shutdown()

		if !testServer1.App.Cluster.Node().IsPrimary() {
			t.Fatalf("Node should be primary")
		}

		testServer2 := test.NewTestServer(t)
		defer testServer2.Shutdown()

		resp, err := testServer2.App.Cluster.Node().Replica().Send(messages.NodeMessage{
			Data: messages.HeartbeatMessage{},
		})

		if err != nil {
			t.Error("Send should not return an error")
		}

		if resp == (messages.NodeMessage{}) {
			t.Errorf("Send should return a response,")
		}
	})
}

func TestNodeReplicaStop(t *testing.T) {
	test.Run(t, func() {
		testServer1 := test.NewTestServer(t)
		defer testServer1.Shutdown()

		if !testServer1.App.Cluster.Node().IsPrimary() {
			t.Fatalf("Node should be primary")
		}

		testServer2 := test.NewTestServer(t)
		defer testServer2.Shutdown()

		err := testServer2.App.Cluster.Node().Replica().Stop()

		if err != nil {
			t.Error("Stop should not return an error")
		}
	})
}
