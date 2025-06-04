package cluster_test

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/litebase/litebase/common/config"
	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/server"
	"github.com/litebase/litebase/server/auth"
	"github.com/litebase/litebase/server/cluster"
)

func TestClusterInit(t *testing.T) {
	test.Run(t, func() {
		os.Setenv("LITEBASE_CLUSTER_ID", "TEST_CLUSTER_000")

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
}

func TestClusterInitNoClusterId(t *testing.T) {
	t.Setenv("LITEBASE_CLUSTER_ID", "")

	// There should be a panic here when we run the test bed so we need to
	// recover from it
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("The code did not panic")
		}
	}()

	test.RunWithApp(t, func(app *server.App) {
	})
}

func TestNewCluster(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		c, err := cluster.NewCluster(app.Config)

		if err != nil {
			t.Fatal(err)
		}

		if c == nil {
			t.Fatal("Cluster is nil")
		}
	})
}

func TestClusterAddMember(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		testServer := test.NewTestServer(t)
		defer testServer.Shutdown()

		c, _ := cluster.NewCluster(app.Config)

		err := c.Save()

		if err != nil {
			t.Fatal(err)
		}

		err = c.AddMember(
			uint64(1), // Using a dummy raft ID for the test
			testServer.Address,
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
			if node.Address == testServer.Address {
				found = true
				break
			}
		}

		if !found {
			t.Fatalf("Member %s not found", testServer.Address)
		}
	})
}

func TestClusterGetMembers(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		c, _ := cluster.NewCluster(app.Config)

		err := c.Save()

		if err != nil {
			t.Fatal(err)
		}

		members := c.GetMembers(true)

		if len(members) != 1 {
			t.Fatal("Members should be 1")
		}
	})
}

func TestClusterGetMembersWithNodes(t *testing.T) {
	test.Run(t, func() {
		server1 := test.NewTestServer(t)
		defer server1.Shutdown()

		members := server1.App.Cluster.GetMembers(true)

		if len(members) != 1 {
			t.Fatal("Members should not be empty")
		}
	})
}

func TestClusterIsMember(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		testServer := test.NewTestServer(t)
		defer testServer.Shutdown()

		// Add a query node
		err := app.Cluster.AddMember(
			uint64(1), // Using a dummy raft ID for the test
			testServer.Address,
		)

		if err != nil {
			t.Fatalf("Error adding query node: %s", err)
		}

		if !app.Cluster.IsMember(testServer.Address, time.Now()) {
			t.Fatal("Node should be a member")
		}
	})
}

func TestClusterRemoveMember(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		testServer := test.NewTestServer(t)
		defer testServer.Shutdown()

		err := app.Cluster.AddMember(
			uint64(1),
			testServer.Address,
		)

		if err != nil {
			t.Fatalf("Error adding query node: %s", err)
		}

		// Verify is a member
		if !app.Cluster.IsMember(testServer.Address, time.Now()) {
			t.Fatal("Node should be a member")
		}

		err = app.Cluster.RemoveMember(testServer.Address, false)

		if err != nil {
			t.Fatalf("Error removing query node: %s", err)
		}

		_, err = app.Cluster.NetworkFS().Stat(app.Cluster.NodePath() + strings.ReplaceAll(testServer.Address, ":", "_"))

		if err != nil {
			t.Error("Query node file should still exist, but got error:", err)
		}
	})
}

func TestClusterRemoveMember_HardState(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		testServer := test.NewTestServer(t)
		defer testServer.Shutdown()

		err := app.Cluster.AddMember(
			uint64(1),
			testServer.Address,
		)

		if err != nil {
			t.Fatalf("Error adding query node: %s", err)
		}

		// Verify is a member
		if !app.Cluster.IsMember(testServer.Address, time.Now()) {
			t.Fatal("Node should be a member")
		}

		err = app.Cluster.RemoveMember(testServer.Address, true)

		if err != nil {
			t.Fatalf("Error removing query node: %s", err)
		}

		_, err = app.Cluster.NetworkFS().Stat(app.Cluster.NodePath() + strings.ReplaceAll(testServer.Address, ":", "_"))

		if err == nil {
			t.Error("Query node file should not exist")
		}
	})
}

func TestClusterSave(t *testing.T) {
	t.Setenv("LITEBASE_CLUSTER_ID", "TEST_CLUSTER_000")

	test.RunWithApp(t, func(app *server.App) {
		c, _ := cluster.NewCluster(app.Config)

		err := c.Save()

		if err != nil {
			t.Fatal(err)
		}

		// Check if the file exists
		dataBytes, err := app.Cluster.ObjectFS().ReadFile(cluster.ConfigPath())

		if err != nil {
			t.Fatal(err)
		}

		data := map[string]any{}

		err = json.Unmarshal(dataBytes, &data)

		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestClusterConfigPath(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		path := cluster.ConfigPath()

		if path != "_cluster/config.json" {
			t.Fatal("Path is not correct")
		}
	})
}

func TestClusterNodePath(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		path := app.Cluster.NodePath()

		if path != "_nodes/" {
			t.Fatalf("Path is not correct: %s", path)
		}
	})
}

func TestClusterPrimaryPathForNode(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		path := app.Cluster.PrimaryPath()

		if path != "_cluster/PRIMARY" {
			t.Fatalf("Path is not correct: %s", path)
		}
	})
}
