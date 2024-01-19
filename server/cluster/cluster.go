package cluster

import (
	"encoding/json"
	"fmt"
	"litebasedb/internal/config"
	"litebasedb/server/storage"
	"os"
)

type ClusterInstance struct {
	Id string
}

var cluster *ClusterInstance

func Get() *ClusterInstance {
	if cluster == nil {
		panic("Cluster not initialized")
	}

	return cluster
}

func Init() (*ClusterInstance, error) {
	// Read the cluster file
	data, err := storage.FS().ReadFile(Path())

	if err != nil {
		if os.IsNotExist(err) {
			if c, err := createClusterFromEnv(); err == nil {
				cluster = c

				return cluster, nil
			}

			return nil, fmt.Errorf("[%d] Cluster has not been initialized", 0)
		}

		return nil, err
	}

	c := &ClusterInstance{}

	err = json.Unmarshal(data, c)

	if err != nil {
		return nil, err
	}

	cluster = c

	return cluster, nil
}

func createClusterFromEnv() (*ClusterInstance, error) {
	clusterId := os.Getenv("LITEBASEDB_CLUSTER_ID")

	if clusterId == "" {
		return nil, fmt.Errorf("[%d] LITEBASEDB_CLUSTER_ID environment variable is not set", 0)
	}

	return NewCluster(clusterId)
}

func NewCluster(id string) (*ClusterInstance, error) {
	// Check if the cluster file already exists
	_, err := storage.FS().Stat(Path())

	if err != nil {
		if !os.IsNotExist(err) {
			panic(err)
			return nil, err
		}
	}

	cluster := &ClusterInstance{
		Id: id,
	}

	cluster.Save()

	return cluster, nil
}

func Path() string {
	return fmt.Sprintf("%s/.litebasedb/cluster.json", config.Get().DataPath)
}

func (cluster *ClusterInstance) Save() error {
	data, err := json.Marshal(cluster)

	if err != nil {
		return err
	}

	err = storage.FS().WriteFile(Path(), data, 0644)

	if err != nil {
		return err
	}

	return nil
}
