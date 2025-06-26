package cluster_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/cluster"
	"github.com/litebase/litebase/pkg/config"
	"github.com/litebase/litebase/pkg/server"
)

func TestCluster(t *testing.T) {
	test.Run(t, func() {
		t.Run("Init", func(t *testing.T) {
			t.Setenv("LITEBASE_CLUSTER_ID", "TEST_CLUSTER_000")

			c := config.NewConfig()

			cluster, err := cluster.NewCluster(c)

			if err != nil {
				t.Fatal(err)
			}

			a := auth.NewAuth(
				c,
				cluster.NetworkFS(),
				cluster.ObjectFS(),
				cluster.TmpFS(),
				cluster.TmpTieredFS(),
			)

			err = cluster.Init(a)

			if err != nil {
				t.Fatal(err)
			}
		})

		t.Run("NewCluster(t *testing.T)", func(t *testing.T) {
			server := test.NewTestServer(t)
			defer server.Shutdown()

			c, err := cluster.NewCluster(server.App.Config)

			if err != nil {
				t.Fatal(err)
			}

			if c == nil {
				t.Fatal("Cluster is nil")
			}
		})

		t.Run("AddMember(t *testing.T)", func(t *testing.T) {
			server1 := test.NewTestServer(t)
			defer server1.Shutdown()

			server2 := test.NewTestServer(t)
			defer server2.Shutdown()

			c, _ := cluster.NewCluster(server1.App.Config)

			err := c.Save()

			if err != nil {
				t.Fatal(err)
			}

			err = c.AddMember(
				"1",
				server2.Address,
			)

			if err != nil {
				t.Fatalf("Error adding query node: %s", err)
			}

			members := c.GetMembers(true)

			if len(members) != 2 {
				t.Fatal("Members should not be empty")
			}

			found := false

			for _, node := range members {
				if node.Address == server2.Address {
					found = true
					break
				}
			}

			if !found {
				t.Fatalf("Member %s not found", server2.Address)
			}
		})

		t.Run("GetMembers", func(t *testing.T) {
			server := test.NewTestServer(t)
			defer server.Shutdown()

			c, _ := cluster.NewCluster(server.App.Config)

			err := c.Save()

			if err != nil {
				t.Fatal(err)
			}

			members := c.GetMembers(true)

			if len(members) != 1 {
				t.Fatal("Members should be 1")
			}
		})

		t.Run("GetMembersWithNodes", func(t *testing.T) {
			server1 := test.NewTestServer(t)
			defer server1.Shutdown()

			members := server1.App.Cluster.GetMembers(true)

			if len(members) != 1 {
				t.Fatal("Members should not be empty")
			}
		})

		t.Run("IsMember", func(t *testing.T) {
			server1 := test.NewTestServer(t)
			defer server1.Shutdown()

			server2 := test.NewTestServer(t)
			defer server2.Shutdown()

			// Add a query node
			err := server1.App.Cluster.AddMember("1", server2.Address)

			if err != nil {
				t.Fatalf("Error adding query node: %s", err)
			}

			if !server1.App.Cluster.IsMember(server2.Address, time.Now().UTC()) {
				t.Fatal("Node should be a member")
			}
		})

		t.Run("RemoveMember", func(t *testing.T) {
			server1 := test.NewTestServer(t)
			defer server1.Shutdown()

			server2 := test.NewTestServer(t)
			defer server2.Shutdown()

			err := server1.App.Cluster.AddMember("1", server2.Address)

			if err != nil {
				t.Fatalf("Error adding query node: %s", err)
			}

			// Verify is a member
			if !server1.App.Cluster.IsMember(server2.Address, time.Now().UTC()) {
				t.Fatal("Node should be a member")
			}

			err = server1.App.Cluster.RemoveMember(server2.Address, false)

			if err != nil {
				t.Fatalf("Error removing query node: %s", err)
			}

			_, err = server1.App.Cluster.NetworkFS().Stat(server1.App.Cluster.NodePath() + strings.ReplaceAll(server2.Address, ":", "_"))

			if err != nil {
				t.Error("Query node file should still exist, but got error:", err)
			}
		})

		t.Run("RemoveMember_HardState", func(t *testing.T) {
			server1 := test.NewTestServer(t)
			defer server1.Shutdown()

			server2 := test.NewTestServer(t)
			defer server2.Shutdown()

			err := server1.App.Cluster.AddMember("1", server2.Address)

			if err != nil {
				t.Fatalf("Error adding query node: %s", err)
			}

			// Verify is a member
			if !server1.App.Cluster.IsMember(server2.Address, time.Now().UTC()) {
				t.Fatal("Node should be a member")
			}

			err = server1.App.Cluster.RemoveMember(server2.Address, true)

			if err != nil {
				t.Fatalf("Error removing query node: %s", err)
			}

			_, err = server1.App.Cluster.NetworkFS().Stat(server1.App.Cluster.NodePath() + strings.ReplaceAll(server2.Address, ":", "_"))

			if err == nil {
				t.Error("Query node file should not exist")
			}
		})

		t.Run("Save", func(t *testing.T) {
			t.Setenv("LITEBASE_CLUSTER_ID", "TEST_CLUSTER_000")
			server := test.NewTestServer(t)
			defer server.Shutdown()

			c, _ := cluster.NewCluster(server.App.Config)

			err := c.Save()

			if err != nil {
				t.Fatal(err)
			}

			// Check if the file exists
			dataBytes, err := server.App.Cluster.ObjectFS().ReadFile(cluster.ConfigPath())

			if err != nil {
				t.Fatal(err)
			}

			data := map[string]any{}

			err = json.Unmarshal(dataBytes, &data)

			if err != nil {
				t.Fatal(err)
			}
		})

		t.Run("ClusterConfigPath", func(t *testing.T) {
			server := test.NewTestServer(t)
			defer server.Shutdown()

			path := cluster.ConfigPath()

			if path != "_cluster/config.json" {
				t.Fatal("Path is not correct")
			}
		})

		t.Run("ClusterNodePath", func(t *testing.T) {
			server := test.NewTestServer(t)
			defer server.Shutdown()

			path := server.App.Cluster.NodePath()

			if path != "_nodes/" {
				t.Fatalf("Path is not correct: %s", path)
			}
		})

		t.Run("ClusterPrimaryPathForNode", func(t *testing.T) {
			server := test.NewTestServer(t)
			defer server.Shutdown()

			path := server.App.Cluster.PrimaryPath()

			if path != "_cluster/PRIMARY" {
				t.Fatalf("Path is not correct: %s", path)
			}
		})
	})
}

func TestCluster_NoClusterId(t *testing.T) {
	test.Run(t, func() {
		t.Run("InitNoClusterId", func(t *testing.T) {
			t.Setenv("LITEBASE_CLUSTER_ID", "")

			// There should be a panic here when we run the test bed so we need to
			// recover from it
			defer func() {
				if r := recover(); r == nil {
					t.Fatal("The code did not panic")
				}
			}()

			c := config.NewConfig()

			c.ClusterId = ""

			server.NewApp(c, nil)
		})
	})
}
