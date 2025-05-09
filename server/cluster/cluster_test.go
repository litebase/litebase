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
			cluster.ObjectFS(),
			cluster.TmpFS(),
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
		testServer := test.NewTestQueryNode(t)

		c, _ := cluster.NewCluster(app.Config)

		err := c.Save()

		if err != nil {
			t.Fatal(err)
		}

		err = c.AddMember(
			config.NodeTypeQuery,
			testServer.Address,
		)

		if err != nil {
			t.Fatalf("Error adding query node: %s", err)
		}

		queryNodes := c.GetMembers(true)

		if len(queryNodes) != 2 {
			t.Fatal("Query nodes should not be empty")
		}

		found := false

		for _, node := range queryNodes {
			if node == testServer.Address {
				found = true
				break
			}
		}

		if !found {
			t.Fatalf("Query node %s not found", testServer.Address)
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

		queryNodes := c.GetMembers(true)

		if len(queryNodes) != 0 {
			t.Fatal("Members should be empty")
		}
	})
}

func TestClusterGetMembersSince(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		testServer := test.NewTestQueryNode(t)
		c, _ := cluster.NewCluster(app.Config)

		err := c.Save()

		if err != nil {
			t.Fatal(err)
		}

		queryNodes := c.GetMembersSince(time.Now())

		if len(queryNodes) != 0 {
			t.Fatal("Members should be empty")
		}

		// Add a query node
		err = app.Cluster.AddMember(
			config.NodeTypeQuery,
			testServer.Address,
		)

		if err != nil {
			t.Errorf("Error adding query node: %s", err)
		}

		_, err = app.Cluster.ObjectFS().Stat(app.Cluster.NodeQueryPath() + strings.ReplaceAll(testServer.Address, ":", "_"))

		if err != nil {
			t.Errorf("Error checking query node file: %s", err)
		}

		queryNodes = c.GetMembersSince(time.Now())

		if len(queryNodes) != 1 {
			t.Fatal("Members should not be empty")
		}

		// Delete the query node file
		err = app.Cluster.ObjectFS().Remove(app.Cluster.NodeQueryPath() + strings.ReplaceAll(testServer.Address, ":", "_"))

		if err != nil {
			t.Errorf("Error deleting query node file: %s", err)
		}

		queryNodes = c.GetMembers(true)

		if len(queryNodes) != 1 {
			t.Fatal("Members should not be empty")
		}

		queryNodes = c.GetMembersSince(time.Now())

		if len(queryNodes) != 0 {
			t.Fatal("Members should be empty")
		}
	})
}

func TestClusterGetMembersWithNodes(t *testing.T) {
	test.Run(t, func() {
		server1 := test.NewTestQueryNode(t)

		queryNodes := server1.App.Cluster.GetMembers(true)

		if len(queryNodes) != 1 {
			t.Fatal("Members should not be empty")
		}
	})
}

func TestClusterIsMember(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		testServer := test.NewTestQueryNode(t)
		c, _ := cluster.NewCluster(app.Config)

		err := c.Save()

		if err != nil {
			t.Fatal(err)
		}

		// Add a query node
		err = c.AddMember(
			config.NodeTypeQuery,
			testServer.Address,
		)

		if err != nil {
			t.Fatalf("Error adding query node: %s", err)
		}

		if !c.IsMember(testServer.Address, time.Now()) {
			t.Fatal("Node should be a member")
		}
	})
}

func TestClusterRemoveMember(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		testServer := test.NewTestQueryNode(t)
		c, _ := cluster.NewCluster(app.Config)

		err := c.Save()

		if err != nil {
			t.Fatal(err)
		}

		err = c.AddMember(
			config.NodeTypeQuery,
			testServer.Address,
		)

		if err != nil {
			t.Fatalf("Error adding query node: %s", err)
		}

		// Verify is a member
		if !c.IsMember(testServer.Address, time.Now()) {
			t.Fatal("Node should be a member")
		}

		err = c.RemoveMember(testServer.Address)

		if err != nil {
			t.Fatalf("Error removing query node: %s", err)
		}

		_, err = app.Cluster.ObjectFS().Stat(app.Cluster.NodeQueryPath() + strings.ReplaceAll(testServer.Address, ":", "_"))

		if err == nil {
			t.Errorf("Query node file should not exist")
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

		data := map[string]interface{}{}

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

func TestClusterLeasePathForQueryNode(t *testing.T) {
	t.Setenv("LITEBASE_NODE_TYPE", config.NodeTypeQuery)

	test.RunWithApp(t, func(app *server.App) {
		path := app.Cluster.LeasePath()

		if path != "_cluster/query/LEASE" {
			t.Fatalf("Path is not correct: %s", path)
		}
	})
}

func TestClusterNodePathForQueryNode(t *testing.T) {
	t.Setenv("LITEBASE_NODE_TYPE", config.NodeTypeQuery)

	test.RunWithApp(t, func(app *server.App) {
		path := app.Cluster.NodePath()

		if path != "_nodes/query/" {
			t.Fatalf("Path is not correct: %s", path)
		}
	})
}

func TestClusterNodeQueryPath(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		path := app.Cluster.NodeQueryPath()

		if path != "_nodes/query/" {
			t.Fatalf("Path is not correct: %s", path)
		}
	})
}

func TestClusterNominationPathForQueryNode(t *testing.T) {
	t.Setenv("LITEBASE_NODE_TYPE", config.NodeTypeQuery)

	test.RunWithApp(t, func(app *server.App) {
		path := app.Cluster.NominationPath()

		if path != "_cluster/query/NOMINATION" {
			t.Fatalf("Path is not correct: %s", path)
		}
	})
}

func TestClusterPrimaryPathForQueryNode(t *testing.T) {
	t.Setenv("LITEBASE_NODE_TYPE", config.NodeTypeQuery)

	test.RunWithApp(t, func(app *server.App) {
		path := app.Cluster.PrimaryPath()

		if path != "_cluster/query/PRIMARY" {
			t.Fatalf("Path is not correct: %s", path)
		}
	})
}
