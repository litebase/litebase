package storage_test

import (
	"litebase/internal/config"
	"litebase/internal/test"
	"litebase/server"
	"litebase/server/storage"
	"testing"

	"github.com/google/uuid"
)

func TestStorageConnectionManager(t *testing.T) {
	test.Run(t, func(app *server.App) {
		scm := storage.SCM()

		if scm == nil {
			t.Fatal("Storage connection manager is nil")
		}
	})
}

func TestStorageConnectionManagerClose(t *testing.T) {
	test.Run(t, func(app *server.App) {
		scm := storage.SCM()

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
	test.Run(t, func(app *server.App) {
		scm := storage.SCM()

		if scm == nil {
			t.Fatal("Storage connection manager is nil")
		}

		connection, err := scm.GetConnection("test")

		if err != storage.ErrNoStorageNodesAvailable {
			t.Fatalf("Expected error %v, got %v", storage.ErrNoStorageNodesAvailable, err)
		}

		if connection != nil {
			t.Fatalf("Expected connection to be nil, got %v", connection)
		}

		err = app.Cluster.AddMember(
			config.NODE_TYPE_STORAGE,
			"10.0.0.0:8080",
		)

		if err != nil {
			t.Fatalf("Error adding storage node: %s", err)
		}

		err = app.Cluster.AddMember(
			config.NODE_TYPE_STORAGE,
			"10.0.0.1:8080",
		)

		if err != nil {
			t.Fatalf("Error adding storage node: %s", err)
		}

		err = app.Cluster.AddMember(
			config.NODE_TYPE_STORAGE,
			"10.0.0.2:8080",
		)

		if err != nil {
			t.Fatalf("Error adding storage node: %s", err)
		}

		connection, err = scm.GetConnection("test1")

		if err != nil {
			t.Fatalf("Error getting connection: %s", err)
		}

		if connection == nil {
			t.Fatal("Connection should not be nil")
		}

		if connection.Index != 1 {
			t.Fatalf("Expected index 1, got %d", connection.Index)
		}

		connection, err = scm.GetConnection("test2")

		if err != nil {
			t.Fatalf("Error getting connection: %s", err)
		}

		if connection == nil {
			t.Fatal("Connection should not be nil")
		}

		if connection.Index != 0 {
			t.Fatalf("Expected index 0, got %d", connection.Index)
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
				keys[i] = append(keys[i], databaseId+"_"+branchId+"_"+rangeFile)
			}
		}
	}

	for i := 0; i < len(databaseIds); i++ {
		t.Run("", func(t *testing.T) {
			test.Run(t, func(app *server.App) {
				testCases := []struct {
					add    []string
					remove []string
				}{
					{
						add: []string{
							"10.0.0.0:8080",
						},
						remove: []string{},
					},
					{
						add: []string{
							"10.0.0.1:8080",
						},
						remove: []string{},
					},
					{
						add: []string{
							"10.0.0.2:8080",
						},
						remove: []string{},
					},
					{

						add: []string{
							"10.0.0.3:8080",
							"10.0.0.4:8080",
							"10.0.0.5:8080",
						},
						remove: []string{},
					},
					{
						add: []string{},
						remove: []string{
							"10.0.0.4:8080",
							"10.0.0.5:8080",
						},
					},
				}

				scm := storage.SCM()
				storageNodeCount := 0

				for _, tc := range testCases {
					// log.Println("--------------------")

					_, storageNodes := app.Cluster.GetMembers(true)

					if len(storageNodes) != storageNodeCount {
						t.Fatalf("Expected %d storage nodes, got %d", storageNodeCount, len(storageNodes))
					}

					if scm == nil {
						t.Fatal("Storage connection manager is nil")
					}

					for _, ip := range tc.add {
						err := app.Cluster.AddMember(config.NODE_TYPE_STORAGE, ip)

						if err != nil {
							t.Fatalf("Error adding storage node: %s", err)
						}
					}

					for _, ip := range tc.remove {
						err := app.Cluster.RemoveMember(ip)

						if err != nil {
							t.Fatalf("Error removing storage node: %s", err)
						}
					}

					storageNodeCount = len(tc.add) + storageNodeCount - len(tc.remove)

					_, storageNodes = app.Cluster.GetMembers(true)

					if len(storageNodes) != storageNodeCount {
						t.Fatalf("Expected %d storage nodes, got %d", storageNodeCount, len(storageNodes))
					}

					nodeDistribution := make(map[string]int)
					keyCount := 0

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

					// Check range of keys received by each node to ensure there
					// is not a great disparity in the distribution
					for _, node := range storageNodes {
						// Check that no node received more than 50% of the keys
						if len(storageNodes) > 2 && nodeDistribution[node] >= keyCount/2 {
							t.Errorf("Node %s received more than 50%% of the keys, received %d%% with %d nodes", node, nodeDistribution[node]*100/keyCount, len(storageNodes))
						}

						// Check that no node received less than 10% of the keys
						if len(storageNodes) > 2 && len(storageNodes) < 6 && nodeDistribution[node] < keyCount/10 {
							t.Errorf("Node %s received less than 10%% of the keys, received %d%% with %d nodes", node, nodeDistribution[node]*100/keyCount, len(storageNodes))
						}
					}
				}
			})
		})
	}
}
