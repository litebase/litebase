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

func TestNode_Address(t *testing.T) {
	test.Run(t, func() {
		c := config.NewConfig()
		clusterInstance, err := cluster.NewCluster(c)
		node := cluster.NewNode(clusterInstance)
		address, _ := node.Address()

		if err != nil {
			t.Fatal(err)
		}

		if address == "" {
			t.Error("Node address not set")
		}

		if address != "127.0.0.1:8080" {
			t.Errorf("Invalid node address: %s expected 127.0.0.1:8080", address)
		}
	})
}

func TestNode_Address_WithAWSEcsProvider(t *testing.T) {
	serverAddress := "192.168.1.1"
	expectedAddress := serverAddress + ":8080"

	// Creatae a mock server to simulate the AWS ECS metadata endpoint
	testServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		w.Write([]byte(`{
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
	}))

	defer testServer.Close()

	t.Setenv("ECS_CONTAINER_METADATA_URI_V4", testServer.URL)

	test.Run(t, func() {
		cluster.SetAddressProvider(nil)

		c := config.NewConfig()
		c.NodeAddressProvider = "aws_ecs"
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
}

func TestNode_AddressPath(t *testing.T) {
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

		expectedPath := fmt.Sprintf("%s127.0.0.1_8080", clusterInstance.NodePath())

		if node.AddressPath() != expectedPath {
			t.Errorf("Invalid node address path: %s expected %s", node.AddressPath(), expectedPath)
		}
	})
}

func TestNode_AddPeerElection(t *testing.T) {
	test.Run(t, func() {
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
}

func TestNode_Context(t *testing.T) {
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

func TestNode_Init(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		// Ensure the directory exists
		_, err := app.Cluster.NetworkFS().Stat(app.Cluster.NodePath())

		if err != nil {
			t.Error(err)
		}

		if app.Cluster.Node().QueryBuilder() == nil {
			t.Error("Query builder not set")
		}
	})
}

func TestNodeIsIdle(t *testing.T) {
	test.Run(t, func() {
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
}

func TestNodeIsPrimary(t *testing.T) {
	test.Run(t, func() {
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
}

func TestNodeIsReplica(t *testing.T) {
	test.Run(t, func() {
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

		time.Sleep(1000 * time.Millisecond) // Wait for the node to be marked as replica

		if server2.App.Cluster.Node().IsReplica() {
			t.Error("Node should not be replica")
		}
	})
}

func TestNode_Primary(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		if server.App.Cluster.Node().Primary() == nil {
			t.Error("Node primary not set")
		}
	})
}

func TestNode_PrimaryAddress(t *testing.T) {
	test.Run(t, func() {
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
}

func TestNode_PrimaryAddressIsEmptyAfterSteppingDown(t *testing.T) {
	test.Run(t, func() {
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
}

func TestNode_Replica(t *testing.T) {
	test.Run(t, func() {
		server1 := test.NewTestServer(t)
		defer server1.Shutdown()
		server2 := test.NewTestServer(t)
		defer server2.Shutdown()

		if server2.App.Cluster.Node().Replica() == nil {
			t.Error("Node replica not set")
		}
	})
}

func TestNode_Send(t *testing.T) {
	test.Run(t, func() {
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
}

func TestNode_SendEvent(t *testing.T) {
	test.Run(t, func() {
		server1 := test.NewTestServer(t)
		defer server1.Shutdown()
		server2 := test.NewTestServer(t)
		defer server2.Shutdown()

		err := server1.App.Cluster.Broadcast("test", "test")

		if err != nil {
			t.Error(err)
		}
	})
}

func TestNodeSetMembership(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		server.App.Cluster.Node().SetMembership(cluster.ClusterMembershipReplica)

		if server.App.Cluster.Node().Membership != cluster.ClusterMembershipReplica {
			t.Error("Node membership not set")
		}
	})
}

func TestNode_SetQueryBuilder(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		queryBuilder := database.NewQueryBuilder(
			server.App.Cluster,
			server.App.Auth.AccessKeyManager,
			server.App.DatabaseManager,
			server.App.LogManager,
		)

		server.App.Cluster.Node().SetQueryBuilder(queryBuilder)

		if server.App.Cluster.Node().QueryBuilder() != queryBuilder {
			t.Error("Query builder not set")
		}
	})
}

func TestNode_SetQueryResponsePool(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		queryResponsePool := database.ResponsePool()
		server.App.Cluster.Node().SetQueryResponsePool(queryResponsePool)

		if server.App.Cluster.Node().QueryResponsePool() != queryResponsePool {
			t.Error("Query response pool not set")
		}
	})
}

func TestNode_SetWALSynchronizer(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		walSynchronizer := database.NewDatabaseWALSynchronizer(server.App.DatabaseManager)
		server.App.Cluster.Node().SetWALSynchronizer(walSynchronizer)

		if server.App.Cluster.Node().WALSynchronizer() != walSynchronizer {
			t.Error("WAL synchronizer not set")
		}
	})
}

func TestNode_Shutdown(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		err := server.App.Cluster.Node().Shutdown()

		if err != nil {
			t.Error("Failed to shutdown node: ", err)
		}
	})
}

func TestNode_Start(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		node := cluster.NewNode(server.App.Cluster)

		timeout := time.After(1 * time.Second)

		select {
		case <-timeout:
			t.Error("Node start timed out")
		case <-node.Start():
			break
		}

		node.Shutdown()
	})
}

func TestNode_StepDown(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		server := app.Cluster.Node()

		if !server.IsPrimary() {
			t.Error("Node should be primary")
		}

		err := server.StepDown()

		if err != nil {
			t.Error("Failed to step down: ", err)
		}

		if server.IsPrimary() {
			t.Error("Node should not be primary after step down")
		}
	})
}

func TestNode_StoreAddress(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		err := server.App.Cluster.Node().StoreAddress()

		if err != nil {
			t.Error("Failed to store address: ", err)
		}
	})
}

func TestNode_Tick(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		lastActive := server.App.Cluster.Node().LastActive
		server.App.Cluster.Node().Tick()

		if server.App.Cluster.Node().LastActive.Equal(lastActive) {
			t.Error("Node last active time not updated")
		}
	})
}

func TestNode_Timestamp(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		timestamp := server.App.Cluster.Node().Timestamp()

		if timestamp.IsZero() {
			t.Error("Node timestamp not set")
		}

	})
}

func TestNode_TickerResumeAfterPause(t *testing.T) {
	test.WithSteps(t, func(sp *test.StepProcessor) {
		sp.Run("PRIMARY_SERVER", func(s *test.StepProcess) {
			defaultNodeTickTimeout := cluster.NodeTickTimeout
			defer func() { cluster.NodeTickTimeout = defaultNodeTickTimeout }()
			cluster.NodeTickTimeout = 500 * time.Millisecond

			test.RunWithoutCleanup(t, func(app *server.App) {
				if !app.Cluster.Node().IsPrimary() {
					t.Fatal("Node is not primary")
				}

				time.Sleep(1 * time.Second)
				s.Step("PRIMARY_READY")
				s.WaitForStep("PRIMARY_RESUMED")

				if app.Cluster.Node().IsPrimary() {
					t.Fatal("Node is still primary after pause")
				}
			})
		})

		sp.Run("PAUSER", func(s *test.StepProcess) {
			s.WaitForStep("PRIMARY_READY")
			s.PauseAndResume("PRIMARY_SERVER", 1*time.Second)
			s.Step("PRIMARY_RESUMED")
		})
	})
}
