package cluster_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/cluster"
	"github.com/litebase/litebase/pkg/cluster/messages"
	"github.com/litebase/litebase/pkg/config"
	"github.com/litebase/litebase/pkg/database"
	"github.com/litebase/litebase/pkg/server"
)

func TestNode(t *testing.T) {
	test.Run(t, func() {
		// Reset port providers to avoid interference from other tests
		cluster.ResetPortProviders()

		t.Run("NewNode", func(t *testing.T) {
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

		t.Run("Address", func(t *testing.T) {
			c := config.NewConfig()
			clusterInstance, err := cluster.NewCluster(c)

			if err != nil {
				t.Fatal(err)
			}

			node := cluster.NewNode(clusterInstance)

			address, err := node.Address()

			if err != nil {
				t.Fatal(err)
			}

			if address == "" {
				t.Error("Node address not set")
			}

			if address != fmt.Sprintf("127.0.0.1:%s", c.PrivatePort) {
				t.Errorf("Invalid node address: %s expected 127.0.0.1:%s", address, c.PrivatePort)
			}
		})

		t.Run("Address_WithAWSEcsProvider", func(t *testing.T) {
			serverAddress := "192.168.1.1"

			// Create a mock server to simulate the AWS ECS metadata endpoint
			testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(200)

				_, err := w.Write([]byte(`{
				"Containers": [
					{
						"Networks": [
							{
								"IPv4Addresses": [
									"` + serverAddress + `"
								]
							}
						]
					}
				]
			}`))

				if err != nil {
					t.Fatal(err)
				}
			}))

			defer testServer.Close()

			t.Setenv("ECS_CONTAINER_METADATA_URI_V4", testServer.URL)

			test.Run(t, func() {
				cluster.SetAddressProvider(nil)

				c := config.NewConfig()
				c.NodeAddressProvider = "aws_ecs"
				expectedAddress := fmt.Sprintf("%s:%s", serverAddress, c.PrivatePort)

				clusterInstance, err := cluster.NewCluster(c)

				if err != nil {
					t.Fatal(err)
				}

				node := cluster.NewNode(clusterInstance)

				address, err := node.Address()

				if err != nil {
					t.Fatal(err)
				}

				if address == "" {
					t.Error("Node address not set")
				}

				if address != expectedAddress {
					t.Errorf("Invalid node address: %s expected %s", address, expectedAddress)
				}
			})
		})

		t.Run("AddPeerElection", func(t *testing.T) {
			c := config.NewConfig()
			clusterInstance, err := cluster.NewCluster(c)

			if err != nil {
				t.Fatal(err)
			}

			node := cluster.NewNode(clusterInstance)

			node.AddPeerElection(&cluster.ClusterElection{})

			if len(node.Elections) != 1 {
				t.Error("Peer election not added")
			}
		})

		t.Run("Context", func(t *testing.T) {
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

		t.Run("Init", func(t *testing.T) {
			server := test.NewTestServer(t)
			defer server.Shutdown()

			// Ensure the directory exists
			_, err := server.App.Cluster.NetworkFS().Stat(server.App.Cluster.NodePath())

			if err != nil {
				t.Error(err)
			}

			if server.App.Cluster.Node().QueryBuilder() == nil {
				t.Error("Query builder not set")
			}
		})

		t.Run("IsIdle", func(t *testing.T) {
			server := test.NewTestServer(t)
			defer server.Shutdown()

			if server.App.Cluster.Node().IsIdle() {
				t.Error("Node should not be idle")
			}

			server.App.Cluster.Node().State = cluster.NodeStateIdle

			if !server.App.Cluster.Node().IsIdle() {
				t.Error("Node should be idle")
			}
		})

		t.Run("IsPrimary", func(t *testing.T) {
			server1 := test.NewTestServer(t)
			defer server1.Shutdown()
			server2 := test.NewTestServer(t)
			defer server2.Shutdown()

			if !server1.App.Cluster.Node().IsPrimary() {
				t.Error("Node should be primary")
			}

			if server2.App.Cluster.Node().IsPrimary() {
				t.Error("Node should not be primary")
			}
		})

		t.Run("IsReplica", func(t *testing.T) {
			server1 := test.NewTestServer(t)

			if !server1.App.Cluster.Node().IsPrimary() {
				t.Error("Node should be primary")
			}

			server2 := test.NewTestServer(t)
			defer server2.Shutdown()

			if !server2.App.Cluster.Node().IsReplica() {
				t.Error("Node should be replica")
			}

			server1.Shutdown()

			// Wait for server2 to detect that server1 is gone and become primary
			// This can take up to 2-3 seconds: 1 second for heartbeat interval + election time
			timeout := time.After(5 * time.Second)
			ticker := time.NewTicker(100 * time.Millisecond)
			defer ticker.Stop()

			for {
				select {
				case <-timeout:
					t.Fatal("Timeout waiting for server2 to become primary")
				case <-ticker.C:
					if !server2.App.Cluster.Node().IsReplica() {
						// Success - server2 is no longer a replica (i.e., it became primary)
						return
					}
				}
			}
		})

		t.Run("Primary", func(t *testing.T) {
			server := test.NewTestServer(t)
			defer server.Shutdown()

			if server.App.Cluster.Node().Primary() == nil {
				t.Error("Node primary not set")
			}
		})

		t.Run("PrimaryAddress", func(t *testing.T) {
			server := test.NewTestServer(t)
			defer server.Shutdown()

			if server.App.Cluster.Node().PrimaryAddress() == "" {
				t.Error("Node primary address not set")
			}

			primaryAddress := server.App.Cluster.Node().PrimaryAddress()
			address, _ := server.App.Cluster.Node().Address()

			if primaryAddress != address {
				t.Error("Node primary address format is invalid")
			}
		})

		t.Run("PrimaryAddressIsEmptyAfterSteppingDown", func(t *testing.T) {
			server := test.NewTestServer(t)
			defer server.Shutdown()

			if server.App.Cluster.Node().PrimaryAddress() == "" {
				t.Error("Node primary address not set")
			}

			primaryAddress := server.App.Cluster.Node().PrimaryAddress()
			address, _ := server.App.Cluster.Node().Address()

			if primaryAddress != address {
				t.Error("Node primary address format is invalid")
			}

			err := server.App.Cluster.Node().StepDown()

			if err != nil {
				t.Error("Failed to step down: ", err)
			}

			if server.App.Cluster.Node().PrimaryAddress() != "" {
				t.Error("Node primary address should be empty after stepping down")
			}
		})

		t.Run("Replica", func(t *testing.T) {
			server1 := test.NewTestServer(t)
			defer server1.Shutdown()
			server2 := test.NewTestServer(t)
			defer server2.Shutdown()

			if server2.App.Cluster.Node().Replica() == nil {
				t.Error("Node replica not set")
			}
		})

		t.Run("Send", func(t *testing.T) {
			server1 := test.NewTestServer(t)
			defer server1.Shutdown()
			server2 := test.NewTestServer(t)
			defer server2.Shutdown()

			address, _ := server2.App.Cluster.Node().Address()

			if !server2.App.Cluster.Node().IsReplica() {
				t.Fatal("Node should not be replica")
			}

			_, err := server2.App.Cluster.Node().Send(
				messages.NodeMessage{
					Data: messages.HeartbeatMessage{
						Address: address,
						ID:      []byte(server2.App.Cluster.Node().ID),
						Time:    time.Now().UTC().Unix(),
					},
				},
			)

			if err != nil {
				t.Error("Failed to send message: ", err)
			}
		})

		t.Run("SendEvent", func(t *testing.T) {
			server1 := test.NewTestServer(t)
			defer server1.Shutdown()
			server2 := test.NewTestServer(t)
			defer server2.Shutdown()

			err := server1.App.Cluster.Broadcast("test", "test")

			if err != nil {
				t.Error(err)
			}
		})

		t.Run("SetMembership", func(t *testing.T) {
			server := test.NewTestServer(t)
			defer server.Shutdown()

			server.App.Cluster.Node().SetMembership(cluster.ClusterMembershipReplica)

			if server.App.Cluster.Node().GetMembership() != cluster.ClusterMembershipReplica {
				t.Error("Node membership not set")
			}
		})

		t.Run("SetQueryBuilder", func(t *testing.T) {
			server := test.NewTestServer(t)
			defer server.Shutdown()

			queryBuilder := database.NewQueryBuilder(
				server.App.Cluster,
				server.App.Auth,
				server.App.DatabaseManager,
				server.App.LogManager,
			)

			server.App.Cluster.Node().SetQueryBuilder(queryBuilder)

			if server.App.Cluster.Node().QueryBuilder() != queryBuilder {
				t.Error("Query builder not set")
			}
		})

		t.Run("SetQueryResponsePool", func(t *testing.T) {
			server := test.NewTestServer(t)
			defer server.Shutdown()

			queryResponsePool := database.ResponsePool()
			server.App.Cluster.Node().SetQueryResponsePool(queryResponsePool)

			if server.App.Cluster.Node().QueryResponsePool() != queryResponsePool {
				t.Error("Query response pool not set")
			}
		})

		t.Run("SetWALSynchronizer", func(t *testing.T) {
			server := test.NewTestServer(t)
			defer server.Shutdown()

			walSynchronizer := database.NewDatabaseWALSynchronizer(server.App.DatabaseManager)
			server.App.Cluster.Node().SetWALSynchronizer(walSynchronizer)

			if server.App.Cluster.Node().WALSynchronizer() != walSynchronizer {
				t.Error("WAL synchronizer not set")
			}
		})

		t.Run("Shutdown", func(t *testing.T) {
			server := test.NewTestServer(t)
			defer server.Shutdown()
		})

		t.Run("Start", func(t *testing.T) {
			server := test.NewUnstartedTestServer(t)

			node := cluster.NewNode(server.App.Cluster)

			timeout := time.After(1 * time.Second)

			select {
			case <-timeout:
				t.Error("Node start timed out")
			case <-node.Start():
				break
			}

			if err := node.Shutdown(); err != nil {
				t.Error("Error shutting down node:", err)
			}
		})

		t.Run("StoreAddress", func(t *testing.T) {
			server := test.NewTestServer(t)
			defer server.Shutdown()

			err := server.App.Cluster.Node().StoreAddress()

			if err != nil {
				t.Error("Failed to store address: ", err)
			}
		})

		t.Run("Tick", func(t *testing.T) {
			server := test.NewTestServer(t)
			defer server.Shutdown()

			lastActive := server.App.Cluster.Node().LastActive
			server.App.Cluster.Node().Tick()

			if server.App.Cluster.Node().LastActive.Equal(lastActive) {
				t.Error("Node last active time not updated")
			}
		})

		t.Run("Timestamp", func(t *testing.T) {
			server := test.NewTestServer(t)
			defer server.Shutdown()

			timestamp := server.App.Cluster.Node().Timestamp()

			if timestamp.IsZero() {
				t.Error("Node timestamp not set")
			}
		})

		t.Run("WaitForPrimary", func(t *testing.T) {
			server := test.NewTestServer(t)
			defer server.Shutdown()

			node := server.App.Cluster.Node()

			// Test 1: Node is already primary
			if !node.IsPrimary() {
				t.Fatal("Node should be primary")
			}

			err := node.WaitForPrimary()

			if err != nil {
				t.Error("WaitForPrimary failed when node is already primary:", err)
			}

			// Test 2: Wait for primary after stepping down
			err = node.StepDown()

			if err != nil {
				t.Fatal("Failed to step down:", err)
			}

			// Give time for a new election to occur
			err = node.WaitForPrimary()

			if err != nil {
				t.Error("WaitForPrimary failed after stepping down:", err)
			}

			// Verify that either this node or another node is primary
			if !node.IsPrimary() && node.PrimaryAddress() == "" {
				t.Error("No primary elected after waiting")
			}
		})
	})
}

func TestNode_StepDown(t *testing.T) {
	test.Run(t, func() {
		// Reset port providers to avoid interference from other tests
		cluster.ResetPortProviders()
		server := test.NewTestServer(t)
		defer server.Shutdown()

		node := server.App.Cluster.Node()

		if !node.IsPrimary() {
			t.Fatal("Node should be primary")
		}

		err := node.StepDown()

		if err != nil {
			t.Error("Failed to step down: ", err)
		}

		if node.IsPrimary() {
			t.Error("Node should not be primary after step down")
		}
	})
}
func TestNode_TickerResumeAfterPause(t *testing.T) {
	// Reset port providers to avoid interference from other tests
	cluster.ResetPortProviders()

	test.WithSteps(t, func(sp *test.StepProcessor) {
		sp.Run("PRIMARY_SERVER", func(s *test.StepProcess) {
			defaultNodeTickTimeout := cluster.NodeTickTimeout
			defer func() { cluster.NodeTickTimeout = defaultNodeTickTimeout }()
			cluster.NodeTickTimeout = 100 * time.Millisecond

			test.RunWithoutCleanup(t, func(app *server.App) {
				if !app.Cluster.Node().IsPrimary() {
					t.Fatal("Node is not primary")
				}

				time.Sleep(1 * time.Second)
				s.Step("PRIMARY_READY")

				if err := s.WaitForStep("PRIMARY_RESUMED"); err != nil {
					t.Fatal(err)
				}

				if app.Cluster.Node().IsPrimary() {
					t.Fatal("Node is still primary after pause")
				}
			})
		})

		sp.Run("PAUSER", func(s *test.StepProcess) {
			if err := s.WaitForStep("PRIMARY_READY"); err != nil {
				t.Fatal(err)
			}

			s.PauseAndResume("PRIMARY_SERVER", 1*time.Second)
			s.Step("PRIMARY_RESUMED")
		})
	})
}
