package cluster_test

import (
	"crypto/sha256"
	"encoding/hex"
	"litebase/internal/test"
	"litebase/server/cluster"
	"litebase/server/cluster/messages"
	"testing"
)

func TestNewNodeReplicationGroup(t *testing.T) {
	test.Run(t, func() {
		testServer := test.NewTestServer(t)
		manager := cluster.NewNodeReplicationGroup(testServer.App.Cluster)

		if manager == nil {
			t.Fatal("Expected manager to be non-nil")
		}
	})
}

func TestNodeReplicationGroupAknowledgeCommit(t *testing.T) {
	test.Run(t, func() {
		nodeCount := 3
		nodes := make([]*test.TestServer, nodeCount)

		for i := 0; i < nodeCount; i++ {
			nodes[i] = test.NewTestStorageNode(t)
		}

		manager := nodes[0].App.Cluster.Node().ReplicationGroupManager

		err := manager.AssignReplicationGroups()

		if err != nil {
			t.Fatalf("Expected no error, got %s", err)
		}

		hash := sha256.Sum256([]byte("world"))
		sha25String := hex.EncodeToString(hash[:])

		err = manager.WriterGroup().Write("hello", []byte("world"))

		if err != nil {
			t.Fatalf("Expected no error, got %s", err)
		}

		err = manager.WriterGroup().Prepare("hello", sha25String)

		if err != nil {
			t.Fatalf("Expected no error, got %s", err)
		}

		for i := 0; i < nodeCount; i++ {
			if i == 0 {
				continue
			}

			// Get the writer replication group
			currentManager := nodes[i].App.Cluster.Node().ReplicationGroupManager

			// Aknowledge the commit
			err := currentManager.WriterGroup().AknowledgeCommit(messages.ReplicationGroupWriteCommitMessage{
				Key:      "hello",
				Proposer: nodes[0].App.Cluster.Node().Address(),
				SHA256:   sha25String,
			})

			if err != nil {
				t.Fatalf("Expected no error, got %s", err)
			}
		}
	})
}

func TestNodeReplicationGroupAknowledgePrepare(t *testing.T) {
	test.Run(t, func() {
		nodeCount := 3
		nodes := make([]*test.TestServer, nodeCount)

		for i := 0; i < nodeCount; i++ {
			nodes[i] = test.NewTestStorageNode(t)
		}

		manager := nodes[0].App.Cluster.Node().ReplicationGroupManager

		err := manager.AssignReplicationGroups()

		if err != nil {
			t.Fatalf("Expected no error, got %s", err)
		}

		hash := sha256.Sum256([]byte("world"))
		sha25String := hex.EncodeToString(hash[:])

		err = manager.WriterGroup().Write("hello", []byte("world"))

		if err != nil {
			t.Fatalf("Expected no error, got %s", err)
		}

		for i := 0; i < nodeCount; i++ {
			if i == 0 {
				continue
			}

			// Get the writer replication group
			currentManager := nodes[i].App.Cluster.Node().ReplicationGroupManager

			// Aknowledge the prepare
			err := currentManager.WriterGroup().AknowledgePrepare(messages.ReplicationGroupWritePrepareMessage{
				Key:      "hello",
				Proposer: nodes[0].App.Cluster.Node().Address(),
				SHA256:   sha25String,
			})

			if err != nil {
				t.Fatalf("Expected no error, got %s", err)
			}
		}
	})
}

func TestNodeReplicationGroupAknowledgeWrite(t *testing.T) {
	test.Run(t, func() {
		nodeCount := 3
		nodes := make([]*test.TestServer, nodeCount)

		for i := 0; i < nodeCount; i++ {
			nodes[i] = test.NewTestStorageNode(t)
		}

		manager := nodes[0].App.Cluster.Node().ReplicationGroupManager

		err := manager.AssignReplicationGroups()

		if err != nil {
			t.Fatalf("Expected no error, got %s", err)
		}

		hash := sha256.Sum256([]byte("world"))
		sha25String := hex.EncodeToString(hash[:])

		for i := 0; i < nodeCount; i++ {
			// Get the writer replication group
			currentManager := nodes[i].App.Cluster.Node().ReplicationGroupManager

			// Aknowledge the write
			err := currentManager.WriterGroup().AknowledgeWrite(messages.ReplicationGroupWriteMessage{
				Key:      "hello",
				Proposer: nodes[0].App.Cluster.Node().Address(),
				SHA256:   sha25String,
			})

			if err != nil {
				t.Fatalf("Expected no error, got %s", err)
			}
		}
	})
}

func TestNodeReplicationGroupCommit(t *testing.T) {
	test.Run(t, func() {
		nodeCount := 3
		nodes := make([]*test.TestServer, nodeCount)

		for i := 0; i < nodeCount; i++ {
			nodes[i] = test.NewTestStorageNode(t)
		}

		manager := nodes[0].App.Cluster.Node().ReplicationGroupManager

		err := manager.AssignReplicationGroups()

		if err != nil {
			t.Fatalf("Expected no error, got %s", err)
		}

		hash := sha256.Sum256([]byte("world"))
		sha25String := hex.EncodeToString(hash[:])

		err = manager.WriterGroup().Write("hello", []byte("world"))

		if err != nil {
			t.Fatalf("Expected no error, got %s", err)
		}

		err = manager.WriterGroup().Prepare("hello", sha25String)

		if err != nil {
			t.Fatalf("Expected no error, got %s", err)
		}

	})
}

func TestNodeReplicationGroupContainsAddress(t *testing.T) {}

func TestNodeReplicationGroupIsObserver(t *testing.T) {
	test.Run(t, func() {
		nodeCount := 4
		nodes := make([]*test.TestServer, nodeCount)

		for i := 0; i < nodeCount; i++ {
			nodes[i] = test.NewTestStorageNode(t)
		}

		manager := nodes[0].App.Cluster.Node().ReplicationGroupManager

		err := manager.AssignReplicationGroups()

		if err != nil {
			t.Fatalf("Expected no error, got %s", err)
		}

		for i := 0; i < nodeCount; i++ {
			if i >= 2 {
				continue
			}

			// Get the writer replication group
			currentManager := nodes[i].App.Cluster.Node().ReplicationGroupManager

			for j, group := range currentManager.ReplicationGroups {
				if j == 0 {
					continue
				}

				if !group.IsObserver() {
					t.Fatalf("Expected node to be an observer")
				}
			}
		}
	})
}

func TestNodeReplicationGroupIsWriter(t *testing.T) {
	test.Run(t, func() {
		nodeCount := 3
		nodes := make([]*test.TestServer, nodeCount)

		for i := 0; i < nodeCount; i++ {
			nodes[i] = test.NewTestStorageNode(t)
		}

		manager := nodes[0].App.Cluster.Node().ReplicationGroupManager

		err := manager.AssignReplicationGroups()

		if err != nil {
			t.Fatalf("Expected no error, got %s", err)
		}

		for i := 0; i < nodeCount; i++ {
			// Get the writer replication group
			currentManager := nodes[i].App.Cluster.Node().ReplicationGroupManager

			if !currentManager.WriterGroup().IsWriter() {
				t.Fatalf("Expected node to be a writer")
			}
		}
	})
}

func TestNodeReplicationGroupPrepare(t *testing.T) {
	test.Run(t, func() {
		nodeCount := 3
		nodes := make([]*test.TestServer, nodeCount)

		for i := 0; i < nodeCount; i++ {
			nodes[i] = test.NewTestStorageNode(t)
		}

		manager := nodes[0].App.Cluster.Node().ReplicationGroupManager

		err := manager.AssignReplicationGroups()

		if err != nil {
			t.Fatalf("Expected no error, got %s", err)
		}

		hash := sha256.Sum256([]byte("world"))
		sha25String := hex.EncodeToString(hash[:])

		err = manager.WriterGroup().Write("hello", []byte("world"))

		if err != nil {
			t.Fatalf("Expected no error, got %s", err)
		}

		err = manager.WriterGroup().Prepare("hello", sha25String)

		if err != nil {
			t.Fatalf("Expected no error, got %s", err)
		}

		for i := 0; i < nodeCount; i++ {
			if i == 0 {
				continue
			}

			// Get the writer replication group
			currentManager := nodes[i].App.Cluster.Node().ReplicationGroupManager
			replicationGroup := currentManager.WriterGroup()

			foundReplicationWrite := false

			for _, write := range replicationGroup.ReplicatedWrites {
				if write.Key == "hello" && write.SHA256 == sha25String && write.PreparedAt > 0 {
					foundReplicationWrite = true
					break
				}
			}

			if !foundReplicationWrite {
				t.Fatalf("Expected to find replicated write in replication group")
			}
		}
	})
}

func TestNodeReplicationGroupSetMembers(t *testing.T) {}

func TestNodeReplicationGroupWrite(t *testing.T) {
	test.Run(t, func() {
		nodeCount := 3
		nodes := make([]*test.TestServer, nodeCount)

		for i := 0; i < nodeCount; i++ {
			nodes[i] = test.NewTestStorageNode(t)
		}

		manager := nodes[0].App.Cluster.Node().ReplicationGroupManager

		err := manager.AssignReplicationGroups()

		if err != nil {
			t.Fatalf("Expected no error, got %s", err)
		}

		err = manager.WriterGroup().Write("hello", []byte("world"))

		if err != nil {
			t.Fatalf("Expected no error, got %s", err)
		}

		for i := 0; i < nodeCount; i++ {
			if i == 0 {
				continue
			}

			// Get the writer replication group
			currentManager := nodes[i].App.Cluster.Node().ReplicationGroupManager
			replicationGroup := currentManager.WriterGroup()

			foundReplicationWrite := false

			for _, write := range replicationGroup.ReplicatedWrites {
				if write.Key == "hello" && write.PreparedAt == 0 {
					foundReplicationWrite = true
					break
				}
			}

			if !foundReplicationWrite {
				t.Fatalf("Expected to find replicated write in replication group")
			}
		}
	})
}
