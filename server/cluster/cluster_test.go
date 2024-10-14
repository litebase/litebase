package cluster_test

import (
	"encoding/json"
	"litebase/internal/config"
	"litebase/internal/test"
	"litebase/server"
	"litebase/server/cluster"
	"litebase/server/storage"
	"os"
	"strings"
	"testing"
	"time"
)

func TestClusterInit(t *testing.T) {
	os.Setenv("LITEBASE_CLUSTER_ID", "TEST_CLUSTER_000")

	test.RunWithApp(t, func(app *server.App) {
		c, err := cluster.Init(app.Auth)

		if err != nil {
			t.Fatal(err)
		}

		if c == nil {
			t.Fatal("Cluster is nil")
		}

		if c.Id != "TEST_CLUSTER_000" {
			t.Fatal("Cluster ID is not correct")
		}

		c, err = cluster.Init(app.Auth)

		if err != nil {
			t.Fatal(err)
		}

		if c == nil {
			t.Fatal("Cluster is nil")
		}

		if c.Id != "TEST_CLUSTER_000" {
			t.Fatal("Cluster ID is not correct")
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
		c, err := cluster.NewCluster("TEST_CLUSTER_002", app.Auth)

		if err != nil {
			t.Fatal(err)
		}

		if c == nil {
			t.Fatal("Cluster is nil")
		}

		if c.Id != "TEST_CLUSTER_002" {
			t.Fatal("Cluster ID is not correct")
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
	t.Setenv("LITEBASE_NODE_TYPE", config.NODE_TYPE_QUERY)

	test.RunWithApp(t, func(app *server.App) {
		path := cluster.LeasePath()

		if path != "_cluster/query/LEASE" {
			t.Fatalf("Path is not correct: %s", path)
		}
	})
}

func TestClusterLeasePathForStorageNode(t *testing.T) {
	t.Setenv("LITEBASE_NODE_TYPE", config.NODE_TYPE_STORAGE)

	test.RunWithApp(t, func(app *server.App) {
		path := cluster.LeasePath()

		if path != "_cluster/storage/LEASE" {
			t.Fatalf("Path is not correct: %s", path)
		}
	})
}

func TestClusterNodePathForQueryNode(t *testing.T) {
	t.Setenv("LITEBASE_NODE_TYPE", config.NODE_TYPE_QUERY)

	test.RunWithApp(t, func(app *server.App) {
		path := cluster.NodePath()

		if path != "_nodes/query/" {
			t.Fatalf("Path is not correct: %s", path)
		}
	})
}

func TestClusterNodePathForStorageNode(t *testing.T) {
	t.Setenv("LITEBASE_NODE_TYPE", config.NODE_TYPE_STORAGE)

	test.RunWithApp(t, func(app *server.App) {
		path := cluster.NodePath()

		if path != "_nodes/storage/" {
			t.Fatalf("Path is not correct: %s", path)
		}
	})
}

func TestClusterNodeQueryPath(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		path := cluster.NodeQueryPath()

		if path != "_nodes/query/" {
			t.Fatalf("Path is not correct: %s", path)
		}
	})
}

func TestClusterNodeStoragePath(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		path := cluster.NodeStoragePath()

		if path != "_nodes/storage/" {
			t.Fatalf("Path is not correct: %s", path)
		}
	})
}

func TestClusterNominationPathForQueryNode(t *testing.T) {
	t.Setenv("LITEBASE_NODE_TYPE", config.NODE_TYPE_QUERY)

	test.RunWithApp(t, func(app *server.App) {
		path := cluster.NominationPath()

		if path != "_cluster/query/NOMINATION" {
			t.Fatalf("Path is not correct: %s", path)
		}
	})
}

func TestClusterNominationPathForStorageNode(t *testing.T) {
	t.Setenv("LITEBASE_NODE_TYPE", config.NODE_TYPE_STORAGE)

	test.RunWithApp(t, func(app *server.App) {
		path := cluster.NominationPath()

		if path != "_cluster/storage/NOMINATION" {
			t.Fatalf("Path is not correct: %s", path)
		}
	})
}

func TestClusterPrimaryPathForQueryNode(t *testing.T) {
	t.Setenv("LITEBASE_NODE_TYPE", config.NODE_TYPE_QUERY)

	test.RunWithApp(t, func(app *server.App) {
		path := cluster.PrimaryPath()

		if path != "_cluster/query/PRIMARY" {
			t.Fatalf("Path is not correct: %s", path)
		}
	})
}

func TestClusterPrimaryPathForStorageNode(t *testing.T) {
	t.Setenv("LITEBASE_NODE_TYPE", config.NODE_TYPE_STORAGE)

	test.RunWithApp(t, func(app *server.App) {
		path := cluster.PrimaryPath()

		if path != "_cluster/storage/PRIMARY" {
			t.Fatalf("Path is not correct: %s", path)
		}
	})
}

func TestClusterSave(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		c, _ := cluster.NewCluster("TEST_CLUSTER_003", app.Auth)

		err := c.Save()

		if err != nil {
			t.Fatal(err)
		}

		// Check if the file exists
		dataBytes, err := storage.ObjectFS().ReadFile(cluster.ConfigPath())

		if err != nil {
			t.Fatal(err)
		}

		data := map[string]interface{}{}

		err = json.Unmarshal(dataBytes, &data)

		if err != nil {
			t.Fatal(err)
		}

		if data["id"] != "TEST_CLUSTER_003" {
			t.Fatal("Cluster ID is not correct")
		}
	})
}

func TestClusterGetMembers(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		c, _ := cluster.NewCluster("TEST_CLUSTER_004", app.Auth)

		err := c.Save()

		if err != nil {
			t.Fatal(err)
		}

		queryNodes, storageNodes := c.GetMembers(true)

		if len(queryNodes) != 0 && len(storageNodes) != 0 {
			t.Fatal("Members should be empty")
		}
	})
}

func TestClusterGetMembersSince(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		c, _ := cluster.NewCluster("TEST_CLUSTER_005", app.Auth)

		err := c.Save()

		if err != nil {
			t.Fatal(err)
		}

		queryNodes, storageNodes := c.GetMembersSince(time.Now())

		if len(queryNodes) != 0 && len(storageNodes) != 0 {
			t.Fatal("Members should be empty")
		}

		// Add a query node
		err = app.Cluster.AddMember(
			config.NODE_TYPE_QUERY,
			"10.0.0.0:8080",
		)

		if err != nil {
			t.Errorf("Error adding query node: %s", err)
		}

		_, err = storage.ObjectFS().Stat(cluster.NodeQueryPath() + strings.ReplaceAll("10.0.0.0:8080", ":", "_"))

		if err != nil {
			t.Errorf("Error checking query node file: %s", err)
		}

		queryNodes, storageNodes = c.GetMembersSince(time.Now())

		if len(queryNodes) != 1 && len(storageNodes) != 0 {
			t.Fatal("Members should not be empty")
		}

		// Delete the query node file
		err = storage.ObjectFS().Remove(cluster.NodeQueryPath() + strings.ReplaceAll("10.0.0.0:8080", ":", "_"))

		if err != nil {
			t.Errorf("Error deleting query node file: %s", err)
		}

		queryNodes, storageNodes = c.GetMembers(true)

		if len(queryNodes) != 1 && len(storageNodes) != 0 {
			t.Fatal("Members should not be empty")
		}

		queryNodes, storageNodes = c.GetMembersSince(time.Now())

		if len(queryNodes) != 0 && len(storageNodes) != 0 {
			t.Fatal("Members should be empty")
		}
	})
}

func TestClusterGetMembersWithNodes(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		c, _ := cluster.NewCluster("TEST_CLUSTER_005", app.Auth)

		err := c.Save()

		if err != nil {
			t.Fatal(err)
		}

		// Add a query node
		err = app.Cluster.AddMember(
			config.NODE_TYPE_QUERY,
			"10.0.0.0:8080",
		)

		if err != nil {
			t.Errorf("Error adding query node: %s", err)
		}

		_, err = storage.ObjectFS().Stat(cluster.NodeQueryPath() + strings.ReplaceAll("10.0.0.0:8080", ":", "_"))

		if err != nil {
			t.Errorf("Error checking query node file: %s", err)
		}

		// Add a storage node
		err = app.Cluster.AddMember(
			config.NODE_TYPE_STORAGE,
			"10.1.0.0:8080",
		)

		if err != nil {
			t.Fatalf("Error adding storage node: %s", err)
		}

		_, err = storage.ObjectFS().Stat(cluster.NodeStoragePath() + strings.ReplaceAll("10.1.0.0:8080", ":", "_"))

		if err != nil {
			t.Errorf("Error checking storage node file: %s", err)
		}

		queryNodes, storageNodes := c.GetMembers(true)

		if len(queryNodes) != 1 && len(storageNodes) != 1 {
			t.Fatal("Members should not be empty")
		}
	})
}

func TestClusterGetStorageNodes(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		c, _ := cluster.NewCluster("TEST_CLUSTER_005", app.Auth)

		err := c.Save()

		if err != nil {
			t.Fatal(err)
		}

		_, address, err := c.GetStorageNode("test")

		if err != storage.ErrNoStorageNodesAvailable {
			t.Fatalf("Expected error %v, got %v", storage.ErrNoStorageNodesAvailable, err)
		}

		if address != "" {
			t.Fatal("Storage nodes should be empty")
		}

		// Add a storage node
		err = c.AddMember(
			config.NODE_TYPE_STORAGE,
			"10.0.0.0:8080",
		)

		if err != nil {
			t.Fatalf("Error adding storage node: %s", err)
		}

		_, err = storage.ObjectFS().Stat(cluster.NodeStoragePath() + strings.ReplaceAll("10.0.0.0:8080", ":", "_"))

		if err != nil {
			t.Errorf("Error checking storage node file: %s", err)
		}

		_, address, err = c.GetStorageNode("test")

		if err != nil {
			t.Fatalf("Error getting storage node: %s", err)
		}

		if address == "" {
			t.Fatal("Storage nodes should not be empty")
		}
	})
}

func TestClusterIsMember(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		c, _ := cluster.NewCluster("TEST_CLUSTER_005", app.Auth)

		err := c.Save()

		if err != nil {
			t.Fatal(err)
		}

		// Add a query node
		err = c.AddMember(
			config.NODE_TYPE_QUERY,
			"10.0.0.0:8080",
		)

		if err != nil {
			t.Fatalf("Error adding query node: %s", err)
		}

		if !c.IsMember("10.0.0.0:8080", time.Now()) {
			t.Fatal("Node should be a member")
		}
	})
}

func TestClusterAddMember(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		c, _ := cluster.NewCluster("TEST_CLUSTER_006", app.Auth)

		err := c.Save()

		if err != nil {
			t.Fatal(err)
		}

		err = c.AddMember(
			config.NODE_TYPE_QUERY,
			"10.0.0.0:8080",
		)

		if err != nil {
			t.Fatalf("Error adding query node: %s", err)
		}

		_, err = storage.ObjectFS().Stat(cluster.NodeQueryPath() + strings.ReplaceAll("10.0.0.0:8080", ":", "_"))

		if err != nil {
			t.Errorf("Error checking query node file: %s", err)
		}
	})
}

func TestClusterRemoveMember(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		c, _ := cluster.NewCluster("TEST_CLUSTER_007", app.Auth)

		err := c.Save()

		if err != nil {
			t.Fatal(err)
		}

		err = c.AddMember(
			config.NODE_TYPE_QUERY,
			"10.0.0.0:8080",
		)

		if err != nil {
			t.Fatalf("Error adding query node: %s", err)
		}

		// Verify is a member
		if !c.IsMember("10.0.0.0:8080", time.Now()) {
			t.Fatal("Node should be a member")
		}

		err = c.RemoveMember("10.0.0.0:8080")

		if err != nil {
			t.Fatalf("Error removing query node: %s", err)
		}

		_, err = storage.ObjectFS().Stat(cluster.NodeQueryPath() + strings.ReplaceAll("10.0.0.0:8080", ":", "_"))

		if err == nil {
			t.Errorf("Query node file should not exist")
		}
	})
}
