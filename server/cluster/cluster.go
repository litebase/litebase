package cluster

import (
	"encoding/json"
	"fmt"
	"litebase/internal/config"
	"litebase/server/storage"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	CLUSTER_MEMBERSHIP_PRIMARY  = "PRIMARY"
	CLUSTER_MEMBERSHIP_REPLICA  = "REPLICA"
	CLUSTER_MEMBERSHIP_STAND_BY = "STAND_BY"
	ELECTION_RETRY_WAIT         = 1 * time.Second

	LEASE_DURATION  = 70 * time.Second
	LEASE_FILE      = "LEASE"
	NOMINATION_FILE = "NOMINATION"
	PRIMARY_FILE    = "PRIMARY"
)

type ClusterInstance struct {
	Id                 string `json:"id"`
	QueryPrimary       string
	QueryNodes         []string
	MembersRetrievedAt time.Time
	StorageNodes       []string
	StoragePrimary     string
}

var (
	cluster *ClusterInstance
	mu      sync.Mutex
)

/*
Get the singleton instance of the cluster.
*/
func Get() *ClusterInstance {
	mu.Lock()
	defer mu.Unlock()

	if cluster == nil {
		panic("cluster not initialized")
	}

	return cluster
}

/*
Initialize the cluster.
*/
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

/*
Create a cluster from the environment variables.
*/
func createClusterFromEnv() (*ClusterInstance, error) {
	clusterId := os.Getenv("LITEBASE_CLUSTER_ID")

	if clusterId == "" {
		return nil, fmt.Errorf("LITEBASE_CLUSTER_ID environment variable is not set")
	}

	return NewCluster(clusterId)
}

/*
Create the directories and files for the cluster.
*/
func createDirectoriesAndFiles() error {
	err := storage.ObjectFS().MkdirAll("_cluster/query", 0755)

	if err != nil {
		return err
	}

	err = storage.ObjectFS().MkdirAll("_cluster/storage", 0755)

	if err != nil {
		return err
	}

	err = storage.ObjectFS().MkdirAll("_nodes/query", 0755)

	if err != nil {
		return err
	}

	err = storage.ObjectFS().MkdirAll("_nodes/storage", 0755)

	if err != nil {
		return err
	}

	if _, err := storage.ObjectFS().Stat(fmt.Sprintf("_cluster/query/%s", NOMINATION_FILE)); os.IsNotExist(err) {
		_, err = storage.ObjectFS().Create(fmt.Sprintf("_cluster/query/%s", NOMINATION_FILE))

		if err != nil {
			return err
		}
	}

	if _, err := storage.ObjectFS().Stat(fmt.Sprintf("_cluster/storage/%s", NOMINATION_FILE)); os.IsNotExist(err) {
		_, err = storage.ObjectFS().Create(fmt.Sprintf("_cluster/storage/%s", NOMINATION_FILE))

		if err != nil {
			return err
		}
	}

	if _, err := storage.ObjectFS().Stat(fmt.Sprintf("_cluster/query/%s", LEASE_FILE)); os.IsNotExist(err) {
		_, err = storage.ObjectFS().Create(fmt.Sprintf("_cluster/query/%s", LEASE_FILE))

		if err != nil {
			return err
		}
	}

	if _, err := storage.ObjectFS().Stat(fmt.Sprintf("_cluster/storage/%s", LEASE_FILE)); os.IsNotExist(err) {
		_, err = storage.ObjectFS().Create(fmt.Sprintf("_cluster/storage/%s", LEASE_FILE))

		if err != nil {
			return err
		}
	}

	if _, err := storage.ObjectFS().Stat(fmt.Sprintf("_cluster/query/%s", PRIMARY_FILE)); os.IsNotExist(err) {
		_, err = storage.ObjectFS().Create(fmt.Sprintf("_cluster/query/%s", PRIMARY_FILE))

		if err != nil {
			return err
		}
	}

	if _, err := storage.ObjectFS().Stat(fmt.Sprintf("_cluster/storage/%s", PRIMARY_FILE)); os.IsNotExist(err) {
		_, err = storage.ObjectFS().Create(fmt.Sprintf("_cluster/storage/%s", PRIMARY_FILE))

		if err != nil {
			return err
		}
	}

	return nil
}

/*
Create a new cluster instance.
*/
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

/*
Return the path to the cluster configuration file.
*/
func ConfigPath() string {
	return "_cluster/config.json"
}

/*
Return the path to the lease file for the cluster, in respect to the node type.
*/
func LeasePath() string {
	return fmt.Sprintf("_cluster/%s/%s", config.Get().NodeType, LEASE_FILE)
}

/*
Return the path to the current node in repsect to the node type.
*/
func NodePath() string {
	return fmt.Sprintf("_nodes/%s/", config.Get().NodeType)
}

/*
Return the path to the query nodes.
*/
func NodeQueryPath() string {
	return fmt.Sprintf("_nodes/%s/", config.NODE_TYPE_QUERY)
}

/*
Return the path to the storage nodes.
*/
func NodeStoragePath() string {
	return fmt.Sprintf("_nodes/%s/", config.NODE_TYPE_STORAGE)
}

/*
Return the path to the nomination file for the cluster, in respect to the node type.
*/
func NominationPath() string {
	return fmt.Sprintf("_cluster/%s/%s", config.Get().NodeType, NOMINATION_FILE)
}

/*
Return the path to the primary file for the cluster, in respect to the node type.
*/
func PrimaryPath() string {
	return fmt.Sprintf("_cluster/%s/%s", config.Get().NodeType, PRIMARY_FILE)
}

/*
Save the cluster configuration.
*/
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

/*
Get all the members of the cluster.
*/
func (cluster *ClusterInstance) GetMembers(cached bool) ([]string, []string) {
	// Return the known nodes if the last retrieval was less than a minute
	if cached && time.Since(cluster.MembersRetrievedAt) < 1*time.Minute {
		return cluster.QueryNodes, cluster.StorageNodes
	}

	// Check if the directory exists
	if _, err := storage.ObjectFS().Stat(NodePath()); os.IsNotExist(err) {
		return nil, nil
	}

	// Read the directory
	files, err := storage.ObjectFS().ReadDir(NodeQueryPath())

	if err != nil {
		return nil, nil
	}

	// Loop through the files
	cluster.QueryNodes = []string{}

	for _, file := range files {
		address := strings.ReplaceAll(file.Name, "_", ":")
		cluster.QueryNodes = append(cluster.QueryNodes, address)
	}

	// Check if the directory exists
	if _, err := storage.ObjectFS().Stat(NodePath()); os.IsNotExist(err) {
		return nil, nil
	}

	// Read the directory
	files, err = storage.ObjectFS().ReadDir(NodeStoragePath())

	if err != nil {
		return nil, nil
	}

	// Loop through the files
	cluster.StorageNodes = []string{}

	for _, file := range files {
		address := strings.ReplaceAll(file.Name, "_", ":")
		cluster.StorageNodes = append(cluster.StorageNodes, address)
	}

	cluster.MembersRetrievedAt = time.Now()

	return cluster.QueryNodes, cluster.StorageNodes
}

/*
Check if the node is a member of the cluster.
*/
func (cluster *ClusterInstance) IsMember(ip string) bool {
	cluster.GetMembers(true)

	for _, member := range cluster.QueryNodes {
		if member == ip {
			return true
		}
	}

	for _, member := range cluster.StorageNodes {
		if member == ip {
			return true
		}
	}

	return false
}

/*
Add a member to the cluster.
*/
func (cluster *ClusterInstance) AddMember(group, ip string) error {
	var err error
	cluster.GetMembers(false)

	if !cluster.IsMember(ip) {
		if group == config.NODE_TYPE_QUERY {
			err = storage.ObjectFS().WriteFile(NodeQueryPath()+strings.ReplaceAll(ip, ":", "_"), []byte(ip), 0644)
		} else {
			err = storage.ObjectFS().WriteFile(NodeStoragePath()+strings.ReplaceAll(ip, ":", "_"), []byte(ip), 0644)
		}
	}

	// Clear the cache
	cluster.MembersRetrievedAt = time.Time{}

	return err
}

/*
Remove a member from the cluster.
*/
func (cluster *ClusterInstance) RemoveMember(ip string) error {
	cluster.GetMembers(true)

	// Clear the cache
	defer func() {
		cluster.MembersRetrievedAt = time.Time{}
	}()

	for i, member := range cluster.QueryNodes {
		if member == ip {
			cluster.QueryNodes = append(cluster.QueryNodes[:i], cluster.QueryNodes[i+1:]...)

			// Remove the node address file
			err := storage.ObjectFS().Remove(NodeQueryPath() + strings.ReplaceAll(ip, ":", "_"))

			if err != nil {
				return err
			}

			break
		}
	}

	for i, member := range cluster.StorageNodes {
		if member == ip {
			cluster.StorageNodes = append(cluster.StorageNodes[:i], cluster.StorageNodes[i+1:]...)

			err := storage.ObjectFS().Remove(NodeStoragePath() + strings.ReplaceAll(ip, ":", "_"))

			if err != nil {
				return err
			}

			break
		}
	}

	return nil
}
