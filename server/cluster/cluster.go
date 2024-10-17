package cluster

import (
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
	ClusterMembershipPrimary = "PRIMARY"
	ClusterMembershipReplica = "REPLICA"
	ClusterMembershipStandBy = "STAND_BY"
	ElectionRetryWait        = 1 * time.Second

	LeaseDuration  = 70 * time.Second
	LeaseFile      = "LEASE"
	Nominationfile = "NOMINATION"
	PrimaryFile    = "PRIMARY"
)

type Cluster struct {
	Auth                     *auth.Auth
	AccessKeyManager         *auth.AccessKeyManager
	channels                 map[string]EventChannel
	Config                   *config.Config
	eventsManager            *EventsManager
	fileSystemMutex          *sync.Mutex
	Id                       string `json:"id"`
	QueryPrimary             string
	QueryNodes               []string
	MembersRetrievedAt       time.Time
	mutex                    *sync.Mutex
	node                     *Node
	StorageConnectionManager *storage.StorageConnectionManager
	StorageNodeHashRing      *storage.StorageNodeHashRing
	StorageNodes             []string
	StoragePrimary           string

	localFileSystem  *storage.FileSystem
	objectFileSystem *storage.FileSystem
	tieredFileSystem *storage.FileSystem
	tmpFileSystem    *storage.FileSystem
}

/*
Create a cluster from the environment variables.
*/
func getClusterIdFromEnv(config *config.Config) (string, error) {
	clusterId := config.ClusterId

	if clusterId == "" {
		return "", fmt.Errorf("LITEBASE_CLUSTER_ID environment variable is not set")
	}

	return clusterId, nil
}

/*
Create the directories and files for the cluster.
*/
func (cluster *Cluster) createDirectoriesAndFiles() error {
	err := cluster.ObjectFS().MkdirAll("_cluster/query", 0755)

	if err != nil {
		return err
	}

	err = cluster.ObjectFS().MkdirAll("_cluster/storage", 0755)

	if err != nil {
		return err
	}

	err = cluster.ObjectFS().MkdirAll("_nodes/query", 0755)

	if err != nil {
		return err
	}

	err = cluster.ObjectFS().MkdirAll("_nodes/storage", 0755)

	if err != nil {
		return err
	}

	if _, err := cluster.ObjectFS().Stat(fmt.Sprintf("_cluster/query/%s", Nominationfile)); os.IsNotExist(err) {
		_, err = cluster.ObjectFS().Create(fmt.Sprintf("_cluster/query/%s", Nominationfile))

		if err != nil {
			return err
		}
	}

	if _, err := cluster.ObjectFS().Stat(fmt.Sprintf("_cluster/storage/%s", Nominationfile)); os.IsNotExist(err) {
		_, err = cluster.ObjectFS().Create(fmt.Sprintf("_cluster/storage/%s", Nominationfile))

		if err != nil {
			return err
		}
	}

	if _, err := cluster.ObjectFS().Stat(fmt.Sprintf("_cluster/query/%s", LeaseFile)); os.IsNotExist(err) {
		_, err = cluster.ObjectFS().Create(fmt.Sprintf("_cluster/query/%s", LeaseFile))

		if err != nil {
			return err
		}
	}

	if _, err := cluster.ObjectFS().Stat(fmt.Sprintf("_cluster/storage/%s", LeaseFile)); os.IsNotExist(err) {
		_, err = cluster.ObjectFS().Create(fmt.Sprintf("_cluster/storage/%s", LeaseFile))

		if err != nil {
			return err
		}
	}

	if _, err := cluster.ObjectFS().Stat(fmt.Sprintf("_cluster/query/%s", PrimaryFile)); os.IsNotExist(err) {
		_, err = cluster.ObjectFS().Create(fmt.Sprintf("_cluster/query/%s", PrimaryFile))

		if err != nil {
			return err
		}
	}

	if _, err := cluster.ObjectFS().Stat(fmt.Sprintf("_cluster/storage/%s", PrimaryFile)); os.IsNotExist(err) {
		_, err = cluster.ObjectFS().Create(fmt.Sprintf("_cluster/storage/%s", PrimaryFile))

		if err != nil {
			return err
		}
	}

	return nil
}

/*
Create a new cluster instance.
*/
func NewCluster(config *config.Config) (*Cluster, error) {
	cluster := &Cluster{
		channels:            map[string]EventChannel{},
		fileSystemMutex:     &sync.Mutex{},
		Config:              config,
		mutex:               &sync.Mutex{},
		StorageNodeHashRing: storage.NewStorageNodeHashRing([]string{}),
	}

	return cluster, nil
}

/*
Add a member to the cluster.
*/
func (cluster *Cluster) AddMember(group, ip string) error {
	defer func() {
		// Clear the cache
		cluster.MembersRetrievedAt = time.Time{}
	}()

	var err error
	cluster.GetMembers(false)

	// if !cluster.IsMember(ip, time.Now()) {
	if group == config.NodeTypeQuery {
		for _, node := range cluster.QueryNodes {
			if node == ip {
				return nil
			}
		}

		cluster.QueryNodes = append(cluster.QueryNodes, ip)
	} else if group == config.NodeTypeStorage {
		for _, node := range cluster.StorageNodes {
			if node == ip {
				return nil
			}
		}

		cluster.StorageNodes = append(cluster.StorageNodes, ip)
		cluster.StorageNodeHashRing.AddNode(ip)
	}
	// }

	return err
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

		if _, err := cluster.ObjectFS().Stat(cluster.NodePath()); os.IsNotExist(err) {
			queryNodesError = err
			return
		}

		// Read the directory
		files, err := cluster.ObjectFS().ReadDir(cluster.NodeQueryPath())

		if err != nil {
			queryNodesError = err
			return
		}

		// Loop through the files
		cluster.QueryNodes = []string{}

		for _, file := range files {
			address := strings.ReplaceAll(file.Name(), "_", ":")
			cluster.QueryNodes = append(cluster.QueryNodes, address)
		}
	}()

	go func() {
		defer wg.Done()

		// Check if the directory exists
		if _, err := cluster.ObjectFS().Stat(cluster.NodePath()); os.IsNotExist(err) {
			storageNodesError = err
			return
		}

		// Read the directory
		files, err := cluster.ObjectFS().ReadDir(cluster.NodeStoragePath())

		if err != nil {
			storageNodesError = err
			return
		}

		// Loop through the files
		cluster.StorageNodes = []string{}
		cluster.StorageNodeHashRing = storage.NewStorageNodeHashRing([]string{})

		for _, file := range files {
			address := strings.ReplaceAll(file.Name(), "_", ":")
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
Initialize the cluster.
*/
func (cluster *Cluster) Init(Auth *auth.Auth) error {
	// Check if the cluster file already exists
	_, err := cluster.ObjectFS().Stat(ConfigPath())

	if err != nil {
		if !os.IsNotExist(err) {
			return err
		}
		id, err := getClusterIdFromEnv(cluster.Config)

		if err != nil {

			return err
		}

		cluster.Id = id
	}

	err = cluster.createDirectoriesAndFiles()

	if err != nil {
		return err
	}

	cluster.Auth = Auth

	cluster.StorageConnectionManager = storage.NewStorageConnectionManager(
		cluster.Config,
	)

	return nil
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

func (c *Cluster) Node() *Node {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.node == nil {
		c.node = NewNode(c)
	}

	return c.node
}

func (c *Cluster) Nodes() []*NodeIdentifier {
	c.GetMembers(true)

	nodes := []*NodeIdentifier{}

	for _, node := range c.QueryNodes {
		nodes = append(nodes, NewNodeIdentifier(node, config.NodeTypeQuery))
	}

	for _, node := range c.StorageNodes {
		nodes = append(nodes, NewNodeIdentifier(node, config.NodeTypeStorage))
	}

	return nodes
}

func (c *Cluster) NodeGroupNodes() []*NodeIdentifier {
	c.GetMembers(true)

	nodes := []*NodeIdentifier{}

	if c.Config.NodeType == config.NodeTypeQuery {
		for _, node := range c.QueryNodes {
			nodes = append(nodes, NewNodeIdentifier(
				strings.Split(node, ":")[0],
				strings.Split(node, ":")[1],
			))
		}
	}

	if c.Config.NodeType == config.NodeTypeStorage {
		for _, node := range c.StorageNodes {
			nodes = append(nodes, NewNodeIdentifier(
				strings.Split(node, ":")[0],
				strings.Split(node, ":")[1],
			))
		}
	}

	return nodes
}

func (c *Cluster) OtherNodes() []*NodeIdentifier {
	nodes := []*NodeIdentifier{}
	address := c.Node().Address()
	c.GetMembers(true)

	for _, node := range c.QueryNodes {
		if node != address {
			nodes = append(nodes, NewNodeIdentifier(
				strings.Split(node, ":")[0],
				strings.Split(node, ":")[1],
			))
		}
	}

	for _, node := range c.StorageNodes {
		if node != address {
			nodes = append(nodes, NewNodeIdentifier(
				strings.Split(node, ":")[0],
				strings.Split(node, ":")[1],
			))
		}
	}

	return nodes
}

func (c *Cluster) OtherQueryNodes() []*NodeIdentifier {
	c.GetMembers(true)

	nodes := []*NodeIdentifier{}
	address := c.Node().Address()

	for _, node := range c.QueryNodes {
		if node != address {
			nodes = append(nodes, NewNodeIdentifier(
				strings.Split(node, ":")[0],
				strings.Split(node, ":")[1],
			))
		}
	}

	return nodes
}

func (c *Cluster) OtherStorageNodes() []*NodeIdentifier {
	c.GetMembers(true)

	nodes := []*NodeIdentifier{}
	address := c.Node().Address()

	for _, node := range c.StorageNodes {
		if node != address {
			nodes = append(nodes, NewNodeIdentifier(
				strings.Split(node, ":")[0],
				strings.Split(node, ":")[1],
			))
		}
	}

	return nodes
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
			err := cluster.ObjectFS().Remove(cluster.NodeQueryPath() + strings.ReplaceAll(ip, ":", "_"))

			if err != nil {
				return err
			}

			break
		}
	}

	for i, member := range cluster.StorageNodes {
		if member == ip {
			cluster.StorageNodes = append(cluster.StorageNodes[:i], cluster.StorageNodes[i+1:]...)

			err := cluster.ObjectFS().Remove(cluster.NodeStoragePath() + strings.ReplaceAll(ip, ":", "_"))

			if err != nil {
				return err
			}

			cluster.StorageNodeHashRing.RemoveNode(ip)

			break
		}
	}

	return nil
}

/*
Return the path to the lease file for the cluster, in respect to the node type.
*/
func (cluster *Cluster) LeasePath() string {
	return fmt.Sprintf("_cluster/%s/%s", cluster.Config.NodeType, LeaseFile)
}

/*
Return the path to the current node in repsect to the node type.
*/
func (cluster *Cluster) NodePath() string {
	return fmt.Sprintf("_nodes/%s/", cluster.Config.NodeType)
}

/*
Return the path to the nomination file for the cluster, in respect to the node type.
*/
func (cluster *Cluster) NominationPath() string {
	return fmt.Sprintf("_cluster/%s/%s", cluster.Config.NodeType, Nominationfile)
}

/*
Return the path to the query nodes.
*/
func (cluster *Cluster) NodeQueryPath() string {
	return fmt.Sprintf("_nodes/%s/", config.NodeTypeQuery)
}

/*
Return the path to the storage nodes.
*/
func (cluster *Cluster) NodeStoragePath() string {
	return fmt.Sprintf("_nodes/%s/", config.NodeTypeStorage)
}

/*
Return the path to the primary file for the cluster, in respect to the node type.
*/
func (cluster *Cluster) PrimaryPath() string {
	return fmt.Sprintf("_cluster/%s/%s", cluster.Config.NodeType, PrimaryFile)
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
	err = cluster.ObjectFS().WriteFile(ConfigPath(), data, 0644)

	if err != nil {
		if os.IsNotExist(err) {
			cluster.ObjectFS().MkdirAll("", 0755)

			goto writefile
		}

		return err
	}

	return cluster.ObjectFS().WriteFile(ConfigPath(), data, 0644)
}

/*
Return the path to the cluster configuration file.
*/
func ConfigPath() string {
	return "_cluster/config.json"
}
