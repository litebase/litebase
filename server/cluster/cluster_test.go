package cluster_test

import (
	"encoding/json"
	"litebasedb/internal/test"
	"litebasedb/server/cluster"
	"os"
	"testing"
)

func TestGetClusterNotInitialized(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("The code did not panic")
		}
	}()

	c := cluster.Get()

	if c != nil {
		t.Fatal("Cluster is not nil")
	}
}

func TestGetCluster(t *testing.T) {
	test.Run(t, func() {
		c := cluster.Get()

		if c == nil {
			t.Fatal("Cluster is nil")
		}
	})
}

func TestClusterInit(t *testing.T) {
	os.Setenv("LITEBASEDB_CLUSTER_ID", "TEST_CLUSTER_000")
	test.Run(t, func() {
		c, err := cluster.Init()

		if err != nil {
			t.Fatal(err)
		}

		if c == nil {
			t.Fatal("Cluster is nil")
		}

		if c.Id != "TEST_CLUSTER_000" {
			t.Fatal("Cluster ID is not correct")
		}

		c, err = cluster.Init()

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
	os.Setenv("LITEBASEDB_CLUSTER_ID", "")

	_, err := cluster.Init()

	if err == nil {
		t.Fatal("Error is nil")
	}

	os.Setenv("LITEBASEDB_CLUSTER_ID", "TEST_CLUSTER_000")
}

func TestNewCluster(t *testing.T) {
	test.Run(t, func() {
		c, err := cluster.NewCluster("TEST_CLUSTER_002")

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

func TestClusterPath(t *testing.T) {
	test.Run(t, func() {
		path := cluster.Path()

		if path != "../../data/_test/.litebasedb/cluster.json" {
			t.Fatal("Path is not correct")
		}
	})
}

func TestClusterSave(t *testing.T) {
	test.Run(t, func() {
		c, _ := cluster.NewCluster("TEST_CLUSTER_003")

		err := c.Save()

		if err != nil {
			t.Fatal(err)
		}

		// Check if the file exists
		dataBytes, err := os.ReadFile(cluster.Path())

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
