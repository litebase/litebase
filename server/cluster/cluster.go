package cluster

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"litebase/internal/config"
	"litebase/server/auth"
	"litebase/server/storage"
	"log"
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

type Cluster struct {
	Auth                *auth.Auth
	AccessKeyManager    *auth.AccessKeyManager
	channels            map[string]EventChannel
	eventsManager       *EventsManager
	Id                  string `json:"id"`
	QueryPrimary        string
	QueryNodes          []string
	MembersRetrievedAt  time.Time
	mutex               *sync.Mutex
	node                *Node
	StorageNodeHashRing *storage.StorageNodeHashRing
	StorageNodes        []string
	StoragePrimary      string
}

/*
Initialize the cluster.
*/
func Init(Auth *auth.Auth) (*Cluster, error) {
	var cluster *Cluster

	err := createDirectoriesAndFiles()

	if err != nil {
		return nil, err
	}

	// Read the cluster file
	data, err := storage.ObjectFS().ReadFile(ConfigPath())

	if err != nil {
		if os.IsNotExist(err) {
			c, err := createClusterFromEnv(Auth)

			if err == nil {
				cluster = c

				return cluster, nil
			}

			return nil, err
		}

		return nil, err
	}

	c := &Cluster{
		Auth:                Auth,
		channels:            map[string]EventChannel{},
		mutex:               &sync.Mutex{},
		StorageNodeHashRing: storage.NewStorageNodeHashRing([]string{}),
	}

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
func createClusterFromEnv(Auth *auth.Auth) (*Cluster, error) {
	clusterId := config.Get().ClusterId

	if clusterId == "" {
		return nil, fmt.Errorf("LITEBASE_CLUSTER_ID environment variable is not set")
	}

	return NewCluster(clusterId, Auth)
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
func NewCluster(id string, Auth *auth.Auth) (*Cluster, error) {
	// Check if the cluster file already exists
	_, err := storage.ObjectFS().Stat(ConfigPath())

	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}
	}

	cluster := &Cluster{
		Auth:                Auth,
		channels:            map[string]EventChannel{},
		Id:                  id,
		mutex:               &sync.Mutex{},
		StorageNodeHashRing: storage.NewStorageNodeHashRing([]string{}),
	}

	err = cluster.Save()

	if err != nil {
		return nil, err
	}

	return cluster, nil
}

func (c *Cluster) Node() *Node {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.node == nil {
		c.node = &Node{
			address:    "",
			cluster:    c,
			lastActive: time.Time{},
			Membership: CLUSTER_MEMBERSHIP_STAND_BY,
			// Membership: CLUSTER_MEMBERSHIP_REPLICA,
			mutex:   &sync.Mutex{},
			standBy: make(chan struct{}),
			State:   NODE_STATE_ACTIVE,
		}

		hash := sha256.Sum256([]byte(c.node.Address()))
		c.node.Id = hex.EncodeToString(hash[:])
		c.node.context, c.node.cancel = context.WithCancel(context.Background())
	}

	return c.node
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
func (cluster *Cluster) Save() error {
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
func (cluster *Cluster) GetMembers(cached bool) ([]string, []string) {
	// Return the known nodes if the last retrieval was less than a minute
	if cached && time.Since(cluster.MembersRetrievedAt) < 1*time.Minute {
		return cluster.QueryNodes, cluster.StorageNodes
	}

	var queryNodesError, storageNodesError error

	wg := sync.WaitGroup{}

	wg.Add(2)

	// Check if the directory exists
	go func() {
		defer wg.Done()

		if _, err := storage.ObjectFS().Stat(NodePath()); os.IsNotExist(err) {
			queryNodesError = err
			return
		}

		// Read the directory
		files, err := storage.ObjectFS().ReadDir(NodeQueryPath())

		if err != nil {
			queryNodesError = err
			return
		}

		// Loop through the files
		cluster.QueryNodes = []string{}

		for _, file := range files {
			address := strings.ReplaceAll(file.Name, "_", ":")
			cluster.QueryNodes = append(cluster.QueryNodes, address)
		}
	}()

	go func() {
		defer wg.Done()

		// Check if the directory exists
		if _, err := storage.ObjectFS().Stat(NodePath()); os.IsNotExist(err) {
			storageNodesError = err
			return
		}

		// Read the directory
		files, err := storage.ObjectFS().ReadDir(NodeStoragePath())

		if err != nil {
			storageNodesError = err
			return
		}

		// Loop through the files
		cluster.StorageNodes = []string{}
		cluster.StorageNodeHashRing = storage.NewStorageNodeHashRing([]string{})

		for _, file := range files {
			address := strings.ReplaceAll(file.Name, "_", ":")
			cluster.StorageNodes = append(cluster.StorageNodes, address)
			cluster.StorageNodeHashRing.AddNode(address)
		}
	}()

	wg.Wait()

	if queryNodesError != nil && !os.IsNotExist(queryNodesError) {
		log.Println("Query nodes error: ", queryNodesError)
		return nil, nil
	}

	if storageNodesError != nil && !os.IsNotExist(storageNodesError) {
		log.Println("Storage nodes error: ", storageNodesError)
		return nil, nil
	}

	cluster.MembersRetrievedAt = time.Now()

	return cluster.QueryNodes, cluster.StorageNodes
}

/*
Get all the members of the cluster since a certain time.
*/
func (cluster *Cluster) GetMembersSince(after time.Time) ([]string, []string) {
	if cluster.MembersRetrievedAt.After(after) {
		return cluster.QueryNodes, cluster.StorageNodes
	}

	return cluster.GetMembers(false)
}

/*
Return a storage node for a given key.
*/
func (cluster *Cluster) GetStorageNode(key string) (int, string, error) {
	cluster.GetMembers(true)

	index, address, err := cluster.StorageNodeHashRing.GetNode(key)

	if err != nil {
		return -1, "", err
	}

	if address == "" {
		return -1, "", storage.ErrNoStorageNodesAvailable
	}

	return index, address, nil
}

/*
Check if the node is a member of the cluster.
*/
func (cluster *Cluster) IsMember(ip string, since time.Time) bool {
	cluster.GetMembersSince(since)

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
func (cluster *Cluster) AddMember(group, ip string) error {
	var err error
	cluster.GetMembers(false)

	if !cluster.IsMember(ip, time.Now()) {
		if group == config.NODE_TYPE_QUERY {
			err = storage.ObjectFS().WriteFile(NodeQueryPath()+strings.ReplaceAll(ip, ":", "_"), []byte(ip), 0644)
		} else {
			err = storage.ObjectFS().WriteFile(NodeStoragePath()+strings.ReplaceAll(ip, ":", "_"), []byte(ip), 0644)

			if err == nil {
				cluster.StorageNodeHashRing.AddNode(ip)
			}
		}
	}

	// Clear the cache
	cluster.MembersRetrievedAt = time.Time{}

	return err
}

/*
Remove a member from the cluster.
*/
func (cluster *Cluster) RemoveMember(ip string) error {
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

			cluster.StorageNodeHashRing.RemoveNode(ip)

			break
		}
	}

	return nil
}
