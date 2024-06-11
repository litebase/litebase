package cluster

import (
	"encoding/json"
	"fmt"
	"litebase/internal/config"
	"litebase/server/storage"
	"os"
	"sync"
)

type ClusterInstance struct {
	Id string `json:"id"`
}

var (
	cluster *ClusterInstance
	mu      sync.Mutex
)

func Get() *ClusterInstance {
	mu.Lock()
	defer mu.Unlock()

	if cluster == nil {
		panic("cluster not initialized")
	}

	return cluster
}

func Init() (*ClusterInstance, error) {
	mu.Lock()
	defer mu.Unlock()

	// Read the cluster file
	data, err := storage.FS().ReadFile(Path())

	if err != nil {
		if os.IsNotExist(err) {
			if c, err := createClusterFromEnv(); err == nil {
				cluster = c

				return cluster, nil
			}

			return nil, err
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
			return nil, err
		}
	}

	cluster := &ClusterInstance{
		Id: id,
	}

	err = cluster.Save()

	if err != nil {
		return nil, err
	}

	return cluster, nil
}

func Path() string {
	return fmt.Sprintf("%s/.litebase/cluster.json", config.Get().DataPath)
}

func (cluster *ClusterInstance) Save() error {
	data, err := json.Marshal(cluster)

	if err != nil {
		return err
	}

writefile:
	err = storage.FS().WriteFile(Path(), data, 0644)

	if err != nil {
		if os.IsNotExist(err) {
			storage.FS().MkdirAll(fmt.Sprintf("%s/.litebase", config.Get().DataPath), 0755)

			goto writefile
		}

		return err
	}

	return storage.FS().WriteFile(Path(), data, 0644)
}
