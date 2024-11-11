package cluster_test

import (
	"litebase/internal/test"
	"litebase/server/cluster"
	"litebase/server/cluster/messages"
	"testing"
)

func TestNewNodeReplicationGroupManagerTest(t *testing.T) {
	test.Run(t, func() {
		testServer := test.NewTestServer(t)
		manager := cluster.NewNodeReplicationGroupManager(testServer.App.Cluster.Node())

		if manager == nil {
			t.Fatal("Expected manager to be non-nil")
		}
	})
}

func TestNodeReplicatinGroupManagerAssignReplicationGroups(t *testing.T) {
	test.Run(t, func() {
		nodeCount := 6
		nodes := make([]*test.TestServer, nodeCount)
		managers := make([]*cluster.NodeReplicationGroupManager, nodeCount)

		for i := 0; i < nodeCount; i++ {
			nodes[i] = test.NewTestStorageNode(t)
			managers[i] = nodes[i].App.Cluster.Node().ReplicationGroupManager
		}

		err := managers[0].AssignReplicationGroups()

		if err != nil {
			t.Fatalf("Expected no error, got %s", err)
		}

		nodesWithReplicationGroupMembers := 0

		for i := 0; i < nodeCount; i++ {
			if i != 0 && nodes[i].App.Cluster.Node().IsPrimary() {
				t.Fatalf("Expected node %d to not be primary", i)
			}

			manager := managers[i]

			if i == 0 && len(manager.Assignments) == 0 {
				t.Fatalf("Expected assignments to be non-empty")
			}

			if i == 0 && len(manager.Assignments) != 2 {
				t.Fatalf("Expected 2 assignment groups, got %d", len(manager.Assignments))
			}

			for j, group := range manager.ReplicationGroups {
				if len(group.Members) != 3 {
					t.Fatalf("Expected 3 nodes in group %d, got %d", j, len(group.Members))
				}

				nodesWithReplicationGroupMembers++
			}
		}

		if nodesWithReplicationGroupMembers != 6 {
			t.Fatalf("Expected 6 nodes with replication group members, got %d", nodesWithReplicationGroupMembers)
		}
	})
}

func TestNodeReplicationGroupManagerFindForAddresses(t *testing.T) {
	test.Run(t, func() {
		nodeCount := 6
		nodes := make([]*test.TestServer, nodeCount)
		managers := make([]*cluster.NodeReplicationGroupManager, nodeCount)

		for i := 0; i < nodeCount; i++ {
			nodes[i] = test.NewTestStorageNode(t)
			managers[i] = nodes[i].App.Cluster.Node().ReplicationGroupManager
		}

		err := managers[0].AssignReplicationGroups()

		if err != nil {
			t.Fatalf("Expected no error, got %s", err)
		}

		nodesWithReplicationGroupMembers := 0

		for i := 0; i < nodeCount; i++ {
			manager := managers[i]

			for _, group := range manager.ReplicationGroups {
				addresses := make([]string, 0)

				for _, member := range group.Members {
					addresses = append(addresses, member.Address)
				}

				foundGroup, err := manager.FindForAddresses(addresses)

				if err != nil {
					t.Fatalf("Expected no error, got %s", err)
				}

				if foundGroup == nil {
					t.Fatalf("Expected group to be found")
				}

				if foundGroup != group {
					t.Fatalf("Expected group to be the same")
				}

				nodesWithReplicationGroupMembers++
			}
		}

		if nodesWithReplicationGroupMembers != 6 {
			t.Fatalf("Expected 6 nodes with replication group members, got %d", nodesWithReplicationGroupMembers)
		}
	})
}

func TestNodeReplicationGroupManagerHandleReplcationGroupAssignmentMessagee(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestStorageNode(t)
		manager := cluster.NewNodeReplicationGroupManager(server.App.Cluster.Node())

		var assignments = [][]messages.ReplicationGroupAssignment{
			{
				{
					Address: server.Address,
					Role:    string(cluster.NodeReplicationGroupWriter),
				},
				{
					Address: "localhost:8001",
					Role:    string(cluster.NodeReplicationGroupWriter),
				},
				{
					Address: "localhost:8002",
					Role:    string(cluster.NodeReplicationGroupWriter),
				},
			},
		}

		err := manager.HandleReplcationGroupAssignmentMessage(messages.ReplicationGroupAssignmentMessage{
			Assignments: assignments,
		})

		if err != nil {
			t.Fatalf("Expected no error, got %s", err)
		}

		if len(manager.ReplicationGroups) != 1 {
			t.Fatalf("Expected 1 replication group, got %d", len(manager.ReplicationGroups))
		}

		group := manager.ReplicationGroups[0]

		if len(group.Members) != 3 {
			t.Fatalf("Expected 3 members, got %d", len(group.Members))
		}

		for i, member := range group.Members {
			if member.Address != assignments[0][i].Address {
				t.Fatalf("Expected address %s, got %s", assignments[0][i].Address, member.Address)
			}

			if member.Role != cluster.NodeReplicationGroupRole(assignments[0][i].Role) {
				t.Fatalf("Expected role %s, got %s", assignments[0][i].Role, member.Role)
			}
		}
	})
}

func TestNodeReplicationGroupManagerWriterGroup(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestStorageNode(t)
		manager := cluster.NewNodeReplicationGroupManager(server.App.Cluster.Node())

		if manager.WriterGroup() == nil {
			t.Fatal("Expected writer group to be non-nil")
		}
	})
}
