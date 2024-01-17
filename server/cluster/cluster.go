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

func Init() *ClusterInstance {
	// Read the cluster file
	data, err := storage.FS().ReadFile(Path())

	if err != nil {
		return nil
	}

	c := &ClusterInstance{}

	err = json.Unmarshal(data, c)

	if err != nil {
		return nil
	}

	cluster = c

	return cluster
}

func NewCluster(id string) (*ClusterInstance, error) {
	// Check if the cluster file already exists
	_, err := storage.FS().Stat(Path())

	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}

		return nil, err
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
