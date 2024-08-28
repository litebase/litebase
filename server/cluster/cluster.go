package cluster

import (
	"encoding/json"
	"fmt"
	"litebase/server/storage"
	"os"
	"sync"
	"time"
)

const (
	CLUSTER_MEMBERSHIP_PRIMARY  = "PRIMARY"
	CLUSTER_MEMBERSHIP_REPLICA  = "REPLICA"
	CLUSTER_MEMBERSHIP_STAND_BY = "STAND_BY"
	ELECTION_RETRY_WAIT         = 1 * time.Second

	LEASE_DURATION  = 5 * time.Second
	LEASE_FILE      = "LEASE"
	NOMINATION_FILE = "NOMINATION"
	PRIMARY_FILE    = "PRIMARY"
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

	err := createDirectoriesAndFiles()

	if err != nil {
		return nil, err
	}

	// Read the cluster file
	data, err := storage.ObjectFS().ReadFile(ConfigPath())

	if err != nil {
		if os.IsNotExist(err) {
			c, err := createClusterFromEnv()

			if err == nil {
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
	clusterId := os.Getenv("LITEBASE_CLUSTER_ID")

	if clusterId == "" {
		return nil, fmt.Errorf("LITEBASE_CLUSTER_ID environment variable is not set")
	}

	return NewCluster(clusterId)
}

func createDirectoriesAndFiles() error {
	err := storage.ObjectFS().MkdirAll("_cluster", 0755)

	if err != nil {
		return err
	}

	err = storage.ObjectFS().MkdirAll("_nodes", 0755)

	if err != nil {
		return err
	}

	if _, err := storage.ObjectFS().Stat(fmt.Sprintf("_cluster/%s", NOMINATION_FILE)); os.IsNotExist(err) {
		_, err = storage.ObjectFS().Create(fmt.Sprintf("_cluster/%s", NOMINATION_FILE))

		if err != nil {
			return err
		}
	}

	if _, err := storage.ObjectFS().Stat(fmt.Sprintf("_cluster/%s", LEASE_FILE)); os.IsNotExist(err) {
		_, err = storage.ObjectFS().Create(fmt.Sprintf("_cluster/%s", LEASE_FILE))

		if err != nil {
			return err
		}
	}

	if _, err := storage.ObjectFS().Stat(fmt.Sprintf("_cluster/%s", PRIMARY_FILE)); os.IsNotExist(err) {
		_, err = storage.ObjectFS().Create(fmt.Sprintf("_cluster/%s", PRIMARY_FILE))

		if err != nil {
			return err
		}
	}

	return nil
}

func NewCluster(id string) (*ClusterInstance, error) {
	// Check if the cluster file already exists
	_, err := storage.ObjectFS().Stat(ConfigPath())

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

func ConfigPath() string {
	return "_cluster/config.json"
}

func LeasePath() string {
	return fmt.Sprintf("_cluster/%s", LEASE_FILE)
}

func NodePath() string {
	return "_nodes"
}

func NominationPath() string {
	return fmt.Sprintf("_cluster/%s", NOMINATION_FILE)
}

func PrimaryPath() string {
	return fmt.Sprintf("_cluster/%s", PRIMARY_FILE)
}

func (cluster *ClusterInstance) Save() error {
	data, err := json.Marshal(cluster)

	if err != nil {
		return err
	}

writefile:
	err = storage.ObjectFS().WriteFile(ConfigPath(), data, 0644)

	if err != nil {
		if os.IsNotExist(err) {
			storage.ObjectFS().MkdirAll("", 0755)

			goto writefile
		}

		return err
	}

	return storage.ObjectFS().WriteFile(ConfigPath(), data, 0644)
}
