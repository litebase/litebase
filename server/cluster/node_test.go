package cluster_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/litebase/litebase/common/config"
	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/server"
	"github.com/litebase/litebase/server/cluster"
	"github.com/litebase/litebase/server/cluster/messages"
	"github.com/litebase/litebase/server/database"
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
		_, err := app.Cluster.ObjectFS().Stat(app.Cluster.NodePath())

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
		server := test.NewTestServer(t)

		if !server.App.Cluster.Node().IsPrimary() {
			t.Error("Node should not be primary")
		}

		server.App.Cluster.Node().Membership = cluster.ClusterMembershipReplica

		if server.App.Cluster.Node().IsPrimary() {
			t.Error("Node should be primary")
		}
	})
}

func TestNodeIsReplica(t *testing.T) {
	test.Run(t, func() {
		server1 := test.NewTestServer(t)
		server2 := test.NewTestServer(t)

		if !server2.App.Cluster.Node().IsReplica() {
			t.Error("Node should be replica")
		}

		server1.Shutdown()

		time.Sleep(3 * time.Second)

		server2.App.Cluster.Node().Membership = cluster.ClusterMembershipReplica

		if server2.App.Cluster.Node().IsReplica() {
			t.Error("Node should not be replica")
		}
	})
}

func TestNodeIsStandBy(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)

		if server.App.Cluster.Node().IsStandBy() {
			t.Error("Node should not be standby")
		}

		server.App.Cluster.Node().Membership = cluster.ClusterMembershipStandBy

		if !server.App.Cluster.Node().IsStandBy() {
			t.Error("Node should be standby")
		}
	})
}

func TestNode_Primary(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)

		if server.App.Cluster.Node().Primary() == nil {
			t.Error("Node primary not set")
		}
	})
}

func TestNode_PrimaryAddress(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)

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

func TestNode_Replica(t *testing.T) {
	test.Run(t, func() {
		test.NewTestServer(t)
		server2 := test.NewTestServer(t)

		if server2.App.Cluster.Node().Replica() == nil {
			t.Error("Node replica not set")
		}
	})
}

func TestNode_RunElection(t *testing.T) {
	for i := 1; i < 12; i++ {
		t.Run(fmt.Sprintf("%d Node Election", i), func(t *testing.T) {
			test.Run(t, func() {
				serverCount := i
				servers := make([]*test.TestServer, serverCount)

				for i := range servers {
					servers[i] = test.NewUnstartedTestServer(t)
				}

				wg := sync.WaitGroup{}
				wg.Add(serverCount)

				for i := range servers {
					go func(server *test.TestServer) {
						defer wg.Done()

						server.App.Cluster.Node().Start()
					}(servers[i])
				}

				wg.Wait()

				ticker := time.NewTicker(1 * time.Second)
				defer ticker.Stop()

				start := time.Now()

				// Ensure only one primary node is elected
				for range ticker.C {
					if time.Since(start) > 10*time.Second {
						t.Fatalf("A primary node was not elected before timeout")
						break
					}

					primaryCount := 0

					for _, server := range servers {
						if server.App.Cluster.Node().IsPrimary() {
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

				for _, server := range servers {
					server.Shutdown()
				}
			})
		})
	}
}

func TestNode_Send(t *testing.T) {
	test.Run(t, func() {
		test.NewTestServer(t)
		server2 := test.NewTestServer(t)
		address, _ := server2.App.Cluster.Node().Address()

		if !server2.App.Cluster.Node().IsReplica() {
			t.Fatal("Node should not be replica")
		}

		_, err := server2.App.Cluster.Node().Send(
			messages.NodeMessage{
				Data: messages.HeartbeatMessage{
					Address: address,
					ID:      []byte(server2.App.Cluster.Node().Id),
					Time:    time.Now().Unix(),
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
		test.NewTestServer(t)

		err := server1.App.Cluster.Broadcast("test", "test")

		if err != nil {
			t.Error(err)
		}
	})
}

func TestNodeSetMembership(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)

		server.App.Cluster.Node().SetMembership(cluster.ClusterMembershipStandBy)

		if server.App.Cluster.Node().Membership != cluster.ClusterMembershipStandBy {
			t.Error("Node membership not set")
		}
	})
}

func TestNode_SetQueryBuilder(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
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
		err := server.App.Cluster.Node().Shutdown()

		if err != nil {
			t.Error("Failed to shutdown node: ", err)
		}
	})
}

func TestNode_Start(t *testing.T) {
	test.Run(t, func() {
		node := cluster.NewNode(test.NewTestServer(t).App.Cluster)

		err := node.Start()

		if err != nil {
			t.Error("Failed to start node: ", err)
		}
	})
}

func TestNode_StoreAddress(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		err := server.App.Cluster.Node().StoreAddress()

		if err != nil {
			t.Error("Failed to store address: ", err)
		}
	})
}

func TestNode_Tick(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		lastActive := server.App.Cluster.Node().LastActive
		server.App.Cluster.Node().Tick()

		if server.App.Cluster.Node().LastActive == lastActive {
			t.Error("Node last active time not updated")
		}
	})
}

func TestNode_Timestamp(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		timestamp := server.App.Cluster.Node().Timestamp()

		if timestamp.IsZero() {
			t.Error("Node timestamp not set")
		}
	})
}

func TestNode_VerifyElectionConfirmation(t *testing.T) {
	test.Run(t, func() {
		server1 := test.NewTestServer(t)
		server2 := test.NewTestServer(t)
		address, _ := server1.App.Cluster.Node().Address()

		confirmed, err := server2.App.Cluster.Node().VerifyElectionConfirmation(
			address,
		)

		if err != nil {
			t.Error("Failed to verify election confirmation: ", err)
		}

		if !confirmed {
			t.Error("Election confirmation not confirmed")
		}
	})
}

func TestNode_VerifyPrimaryStatus(t *testing.T) {
	test.Run(t, func() {
		server1 := test.NewTestServer(t)
		server2 := test.NewTestServer(t)

		server1IsPrimary := server1.App.Cluster.Node().VerifyPrimaryStatus()
		server2IsPrimary := server2.App.Cluster.Node().VerifyPrimaryStatus()

		if !server1IsPrimary {
			t.Error("Server 1 should not be primary")
		}

		if server2IsPrimary {
			t.Error("Server 2 should not be primary")
		}
	})
}
