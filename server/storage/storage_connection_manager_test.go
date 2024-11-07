package storage_test

import (
	"litebase/internal/test"
	"litebase/server"
	"litebase/server/storage"
	"log"
	"testing"

	"github.com/google/uuid"
)

func TestNewStorageConnectionManager(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		scm := storage.NewStorageConnectionManager(app.Cluster.Config)

		if scm == nil {
			t.Fatal("Storage connection manager is nil")
		}
	})
}

func TestStorageConnectionManagerClose(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		scm := storage.NewStorageConnectionManager(app.Cluster.Config)

		if scm == nil {
			t.Fatal("Storage connection manager is nil")
		}

		errors := scm.Close()

		if len(errors) != 0 {
			t.Fatal("Errors closing storage connections")
		}
	})
}

func TestStorageConnectionManagerGetConnection(t *testing.T) {
	test.Run(t, func() {
		queryNode := test.NewTestQueryNode(t)

		scm := storage.NewStorageConnectionManager(queryNode.App.Cluster.Config)

		if scm == nil {
			t.Fatal("Storage connection manager is nil")
		}

		connection, err := scm.GetConnection("test")

		if err != storage.ErrNoStorageNodesAvailable {
			t.Fatalf("Expected error %v, got %v", storage.ErrNoStorageNodesAvailable, err)
		}

		test.NewTestStorageNode(t)
		test.NewTestStorageNode(t)
		test.NewTestStorageNode(t)

		if connection != nil {
			t.Fatalf("Expected connection to be nil, got %v", connection)
		}

		connection, err = scm.GetConnection("test1")

		if err != nil {
			t.Fatalf("Error getting connection: %s", err)
		}

		if connection == nil {
			t.Fatal("Connection should not be nil")
		}

		connection, err = scm.GetConnection("test2")

		if err != nil {
			t.Fatalf("Error getting connection: %s", err)
		}

		if connection == nil {
			t.Fatal("Connection should not be nil")
		}
	})
}

func TestStorageConnectionManagerGetConnectionWithChangingMembership(t *testing.T) {
	databaseIds := []string{}

	for i := 0; i < 5; i++ {
		databaseIds = append(databaseIds, uuid.NewString())
	}

	// Create 5 uuids
	branchIds := []string{}

	for i := 0; i < 5; i++ {
		branchIds = append(branchIds, uuid.NewString())
	}

	rangeFiles := []string{
		"0000000001",
		"0000000002",
		"0000000003",
		"0000000004",
		"0000000005",
		"0000000006",
		"0000000007",
		"0000000008",
		"0000000009",
		"0000000010",
	}

	keys := make(map[int][]string)

	for i, databaseId := range databaseIds {
		for _, branchId := range branchIds {
			for _, rangeFile := range rangeFiles {
				keys[i] = append(keys[i], databaseId+"/"+branchId+"/"+rangeFile)
			}
		}
	}

	for i := 0; i < len(databaseIds); i++ {
		t.Run("", func(t *testing.T) {
			var storageNodeServers []*test.TestServer

			test.Run(t, func() {
				queryNode := test.NewTestQueryNode(t)

				testCases := []struct {
					add    int
					remove int
				}{
					{
						add:    1,
						remove: 0,
					},
					{
						add:    2,
						remove: 0,
					},
					{
						add:    2,
						remove: 0,
					},
					{
						add:    2,
						remove: 0,
					},
					// {
					// 	add:    0,
					// 	remove: 2,
					// },
					// {
					// 	add:    0,
					// 	remove: 2,
					// },
				}

				scm := queryNode.App.Cluster.StorageConnectionManager

				if scm == nil {
					t.Fatal("Storage connection manager is nil")
				}

				storageNodeCount := 0

				for _, tc := range testCases {
					_, storageNodes := queryNode.App.Cluster.GetMembers(false)

					if len(storageNodes) != storageNodeCount {
						t.Fatalf("Expected %d storage nodes, got %d", storageNodeCount, len(storageNodes))
					}

					for i := 0; i < tc.add; i++ {
						storageNodeServers = append(storageNodeServers, test.NewTestStorageNode(t))
						storageNodeCount++
					}

					for i := 0; i < tc.remove; i++ {
						storageNodeServers[i].Shutdown()
						storageNodeCount--
					}

					_, storageNodes = queryNode.App.Cluster.GetMembers(false)

					if len(storageNodes) != storageNodeCount {
						log.Println(storageNodes)
						t.Fatalf("Expected %d storage nodes, got %d", storageNodeCount, len(storageNodes))
					}

					keyCount := 0
					nodeDistribution := make(map[string]int)

					for keyIndex, keyGroup := range keys {
						if keyIndex > i {
							continue
						}

						for _, key := range keyGroup {
							connection, err := scm.GetConnection(key)

							if err != nil {
								t.Fatalf("Error getting connection: %s", err)
							}

							if connection == nil {
								t.Fatalf("Connection should not be nil for key %s", key)
							}

							nodeDistribution[connection.Address]++
							keyCount++
						}
					}

					majorityThreshold := (keyCount + 1) / 2
					majorityPercentage := (float64(majorityThreshold) / float64(keyCount)) * 100

					// Check range of keys received by each node to ensure there
					// is not a great disparity in the distribution
					for _, node := range storageNodes {
						// Check that no node received more than a majority of the keys
						if len(storageNodes) > 2 && nodeDistribution[node] > majorityThreshold {
							t.Errorf(
								"node %s received more than %.2f%% of the keys with %d nodes: %d out of %d - %d%%",
								node, majorityPercentage, len(storageNodes), keyCount, keyCount, keyCount*100/keyCount,
							)
						}

						// Check that no node received less than 10% of the keys
						// if len(storageNodes) > 2 && len(storageNodes) < 6 && nodeDistribution[node] < keyCount/10 {
						// 	t.Errorf("Node %s received less than 10%% of the keys, received %d%% with %d nodes", node, nodeDistribution[node]*100/keyCount, len(storageNodes))
						// }
					}
				}

				queryNode.Shutdown()

				for _, storageNodeServer := range storageNodeServers {
					storageNodeServer.Shutdown()
				}

			})
		})
	}
}
