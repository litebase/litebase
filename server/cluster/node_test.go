package cluster_test

import (
	"fmt"
	"litebase/internal/config"
	"litebase/internal/test"
	"litebase/server"
	"litebase/server/cluster"
	"sync"
	"testing"
	"time"
)

func TestNewNode(t *testing.T) {
	test.Run(t, func() {
		c := config.NewConfig()
		clusterInstance, err := cluster.NewCluster(c)

		if err != nil {
			t.Fatal(err)
		}

		node := cluster.NewNode(clusterInstance)

		if node == nil {
			t.Error("Node not created")
		}
	})
}

func TestNodeAddress(t *testing.T) {
	test.Run(t, func() {
		c := config.NewConfig()
		clusterInstance, err := cluster.NewCluster(c)

		if err != nil {
			t.Fatal(err)
		}

		node := cluster.NewNode(clusterInstance)

		if node.Address() == "" {
			t.Error("Node address not set")
		}

		if node.Address() != "localhost:8080" {
			t.Errorf("Invalid node address: %s expected localhost:8080", node.Address())
		}
	})
}

func TestNodeAddressPath(t *testing.T) {
	test.Run(t, func() {
		c := config.NewConfig()
		clusterInstance, err := cluster.NewCluster(c)

		if err != nil {
			t.Fatal(err)
		}

		node := cluster.NewNode(clusterInstance)

		if node.AddressPath() == "" {
			t.Error("Node address path not set")
		}

		expectedPath := fmt.Sprintf("%slocalhost_8080", clusterInstance.NodePath())

		if node.AddressPath() != expectedPath {
			t.Errorf("Invalid node address path: %s expected %s", node.AddressPath(), expectedPath)
		}
	})
}

func TestNodeContext(t *testing.T) {
	test.Run(t, func() {
		c := config.NewConfig()
		clusterInstance, err := cluster.NewCluster(c)

		if err != nil {
			t.Fatal(err)
		}

		node := cluster.NewNode(clusterInstance)

		if node.Context() == nil {
			t.Error("Node context not set")
		}
	})
}

func TestNodeHeartbeat(t *testing.T) {}

func TestNodeInit(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		// Ensure the directory exists
		_, err := app.Cluster.ObjectFS().Stat(app.Cluster.NodePath())

		if err != nil {
			t.Error(err)
		}

		if app.Cluster.Node().QueryBuilder() == nil {
			t.Error("Query builder not set")
		}

		if app.Cluster.Node().WalSynchronizer() == nil {
			t.Error("Wal synchronizer not set")
		}
	})
}

func TestNodeIsIdle(t *testing.T) {}

func TestNodeIsPrimary(t *testing.T) {}

func TestNodeIsReplica(t *testing.T) {}

func TestNodeIsStandBy(t *testing.T) {}

func TestNodeOtherNodes(t *testing.T) {}

func TestNodeOtherQueryNodes(t *testing.T) {}

func TestNodeOtherStorageNodes(t *testing.T) {}

func TestNodePrimary(t *testing.T) {}

func TestNodePrimaryAddress(t *testing.T) {}

func TestNodePublish(t *testing.T) {}

func TestNodeReplica(t *testing.T) {}

func TestNodeWalReplicator(t *testing.T) {}

func TestNodeRunElection(t *testing.T) {
	for i := 1; i < 12; i++ {
		t.Run(fmt.Sprintf("%d Node Election", i), func(t *testing.T) {
			test.Run(t, func() {
				nodeCount := i
				nodes := make([]*test.TestServer, nodeCount)

				for i := 0; i < nodeCount; i++ {
					nodes[i] = test.NewUnstartedTestServer(t)
				}

				wg := sync.WaitGroup{}
				wg.Add(nodeCount)

				for i := 0; i < nodeCount; i++ {
					go func(node *test.TestServer) {
						defer wg.Done()
						node.App.Cluster.Node().Start()
					}(nodes[i])
				}

				wg.Wait()

				ticker := time.NewTicker(2 * time.Second)
				defer ticker.Stop()

				start := time.Now()
				// Ensure only one primary node is elected

				for range ticker.C {
					if time.Since(start) > 3*time.Second {
						t.Fatalf("A primary node was not elected before timeout")
						break
					}
					primaryCount := 0

					for i := 0; i < nodeCount; i++ {
						if nodes[i].App.Cluster.Node().IsPrimary() {
							primaryCount++
						}
					}

					if primaryCount > 1 {
						t.Fatalf("Invalid primary count: %d expected 1", primaryCount)
						break
					}

					if primaryCount == 1 {
						break
					}
				}
			})
		})
	}
}

func TestNodeSetQueryBuilder(t *testing.T) {}

func TestNodeSetWalSynchronizer(t *testing.T) {}

func TestNodeShutdown(t *testing.T) {}

func TestNodeStart(t *testing.T) {}

func TestNodeTick(t *testing.T) {}
