package cluster

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/litebase/litebase/common/config"
	"github.com/litebase/litebase/server/auth"
	"github.com/litebase/litebase/server/storage"
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
	Auth               *auth.Auth
	AccessKeyManager   *auth.AccessKeyManager
	channels           map[string]EventChannel
	Config             *config.Config
	eventsManager      *EventsManager
	fileSystemMutex    *sync.Mutex
	Initialized        bool
	Id                 string `json:"id"`
	nodeMap            map[string]map[string]struct{}
	QueryPrimary       string
	queryNodes         []string
	MembersRetrievedAt time.Time
	mutex              *sync.Mutex
	node               *Node

	localFileSystem  *storage.FileSystem
	objectFileSystem *storage.FileSystem
	sharedFileSystem *storage.FileSystem
	tieredFileSystem *storage.FileSystem
	tmpFileSystem    *storage.FileSystem
}

// Create a cluster from the environment variables.
func getClusterIdFromEnv(config *config.Config) (string, error) {
	clusterId := config.ClusterId

	if clusterId == "" {
		return "", fmt.Errorf("LITEBASE_CLUSTER_ID environment variable is not set")
	}

	return clusterId, nil
}

// Create the directories and files for the cluster.
func (cluster *Cluster) createDirectoriesAndFiles() error {
	err := cluster.SharedFS().MkdirAll("_cluster/query", 0755)

	if err != nil {
		return err
	}

	err = cluster.SharedFS().MkdirAll(cluster.NodePath(), 0755)

	if err != nil {
		return err
	}

	err = cluster.SharedFS().MkdirAll(cluster.NodeQueryPath(), 0755)

	if err != nil {
		return err
	}

	if _, err := cluster.SharedFS().Stat(fmt.Sprintf("_cluster/query/%s", Nominationfile)); os.IsNotExist(err) {
		_, err = cluster.SharedFS().Create(fmt.Sprintf("_cluster/query/%s", Nominationfile))

		if err != nil {
			return err
		}
	}

	if _, err := cluster.SharedFS().Stat(fmt.Sprintf("_cluster/query/%s", LeaseFile)); os.IsNotExist(err) {
		_, err = cluster.SharedFS().Create(fmt.Sprintf("_cluster/query/%s", LeaseFile))

		if err != nil {
			return err
		}
	}

	if _, err := cluster.SharedFS().Stat(fmt.Sprintf("_cluster/query/%s", PrimaryFile)); os.IsNotExist(err) {
		_, err = cluster.SharedFS().Create(fmt.Sprintf("_cluster/query/%s", PrimaryFile))

		if err != nil {
			return err
		}
	}

	return nil
}

// Create a new cluster instance.
func NewCluster(config *config.Config) (*Cluster, error) {
	cluster := &Cluster{
		channels:        map[string]EventChannel{},
		fileSystemMutex: &sync.Mutex{},
		Config:          config,
		mutex:           &sync.Mutex{},
		nodeMap:         map[string]map[string]struct{}{},
	}

	return cluster, nil
}

// Add a member to the cluster.
func (cluster *Cluster) AddMember(group, ip string) error {
	var err error
	cluster.GetMembers(false)

	cluster.mutex.Lock()

	defer func() {
		cluster.mutex.Unlock()
		// Clear the cache
		cluster.MembersRetrievedAt = time.Time{}
	}()

	if group == config.NodeTypeQuery {
		if slices.Contains(cluster.queryNodes, ip) {
			return nil
		}

		cluster.nodeMap[config.NodeTypeQuery][ip] = struct{}{}
	}

	return err
}

func (cluster *Cluster) AllQueryNodes() []*NodeIdentifier {
	cluster.GetMembers(true)

	identifiers := make([]*NodeIdentifier, len(cluster.queryNodes))

	for i, node := range cluster.queryNodes {
		identifiers[i] = NewNodeIdentifier(
			strings.Split(node, ":")[0],
			strings.Split(node, ":")[1],
		)
	}

	return identifiers
}

// Get all the members of the cluster.
func (cluster *Cluster) GetMembers(cached bool) []string {
	cluster.mutex.Lock()
	defer cluster.mutex.Unlock()

	// Return the known nodes if the last retrieval was less than a minute
	if cached && time.Since(cluster.MembersRetrievedAt) < 1*time.Minute {
		return cluster.queryNodes
	}

	cluster.nodeMap = map[string]map[string]struct{}{
		config.NodeTypeQuery: {},
	}

	// if !cluster.Initialized || !cluster.node.initialized {
	// 	return nil
	// }

	_, err := cluster.SharedFS().Stat(cluster.NodePath())

	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}

		log.Println("Error reading cluster nodes: ", err, cluster.NodePath())
		return nil
	}

	// Read the directory
	files, err := cluster.SharedFS().ReadDir(cluster.NodeQueryPath())

	if err != nil {
		log.Println("Error reading query nodes: ", err)
		return nil
	}

	// Loop through the files and store the node addresses
	for _, file := range files {
		address := strings.ReplaceAll(file.Name(), "_", ":")
		cluster.nodeMap[config.NodeTypeQuery][address] = struct{}{}
	}

	cluster.queryNodes = []string{}

	for node := range cluster.nodeMap[config.NodeTypeQuery] {
		cluster.queryNodes = append(cluster.queryNodes, node)
	}

	cluster.MembersRetrievedAt = time.Now()

	return cluster.queryNodes
}

// Get all the members of the cluster since a certain time.
func (cluster *Cluster) GetMembersSince(after time.Time) []string {
	if cluster.MembersRetrievedAt.After(after) {
		return cluster.queryNodes
	}

	return cluster.GetMembers(false)
}

// Initialize the cluster.
func (cluster *Cluster) Init(Auth *auth.Auth) error {
	// Check if the cluster file already exists
	_, err := cluster.ObjectFS().Stat(ConfigPath())

	if err != nil {
		if os.IsNotExist(err) {
			err := cluster.ObjectFS().MkdirAll(filepath.Dir(ConfigPath()), 0755)

			if err != nil {
				return err
			}
		}

		id, err := getClusterIdFromEnv(cluster.Config)

		if err != nil {
			return err
		}

		cluster.Id = id

		err = cluster.Save()

		if err != nil {
			return err
		}
	}

	clusterFile, err := cluster.ObjectFS().ReadFile(ConfigPath())

	if err != nil && !os.IsNotExist(err) {
		return err
	}

	if err == nil {
		err = json.Unmarshal(clusterFile, cluster)

		if err != nil {
			return err
		}
	}

	err = cluster.createDirectoriesAndFiles()

	if err != nil {
		log.Println("Error creating directories and files: ", err)
		return err
	}

	cluster.Auth = Auth
	cluster.Initialized = true

	return nil
}

// Check if the node is a member of the cluster.
func (cluster *Cluster) IsMember(ip string, since time.Time) bool {
	cluster.GetMembersSince(since)

	return slices.Contains(cluster.queryNodes, ip)
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

	for _, node := range c.queryNodes {
		nodes = append(nodes, NewNodeIdentifier(node, config.NodeTypeQuery))
	}

	return nodes
}

func (c *Cluster) NodeGroupNodes() []*NodeIdentifier {
	c.GetMembers(true)

	nodes := []*NodeIdentifier{}

	if c.Config.NodeType == config.NodeTypeQuery {
		for _, node := range c.queryNodes {
			nodes = append(nodes, NewNodeIdentifier(
				strings.Split(node, ":")[0],
				strings.Split(node, ":")[1],
			))
		}
	}

	return nodes
}

func (c *Cluster) NodeGroupVotingNodes() []*NodeIdentifier {
	c.GetMembers(false)

	nodes := c.NodeGroupNodes()

	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].String() < nodes[j].String()
	})

	// Ensure an odd number of nodes for majority voting
	if len(nodes) > 5 {
		if len(nodes[:5])%2 == 0 {
			return nodes[:5+1]
		}

		return nodes[:5]
	}

	if len(nodes)%2 == 0 && len(nodes) > 1 {
		return nodes[:len(nodes)-1]
	}

	return nodes
}

func (c *Cluster) OtherNodes() []*NodeIdentifier {
	nodes := []*NodeIdentifier{}
	address, _ := c.Node().Address()
	c.GetMembers(true)

	for _, node := range c.queryNodes {
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
	address, _ := c.Node().Address()

	for _, node := range c.queryNodes {
		if node != address {
			nodes = append(nodes, NewNodeIdentifier(
				strings.Split(node, ":")[0],
				strings.Split(node, ":")[1],
			))
		}
	}

	return nodes
}

// Remove a member from the cluster.
func (cluster *Cluster) RemoveMember(address string) error {
	cluster.GetMembers(true)
	cluster.mutex.Lock()

	// Clear the cache
	defer func() {
		cluster.mutex.Unlock()
		cluster.MembersRetrievedAt = time.Time{}
	}()

	for _, member := range cluster.queryNodes {
		if member == address {
			delete(cluster.nodeMap[config.NodeTypeQuery], address)

			// Remove the node address file
			err := cluster.SharedFS().Remove(cluster.NodeQueryPath() + strings.ReplaceAll(address, ":", "_"))

			if err != nil {
				return err
			}

			break
		}
	}

	return nil
}

// Return the path to the lease file for the cluster, in respect to the node type.
func (cluster *Cluster) LeasePath() string {
	return fmt.Sprintf("_cluster/%s/%s", cluster.Config.NodeType, LeaseFile)
}

// Return the path to the current node in repsect to the node type.
func (cluster *Cluster) NodePath() string {
	return fmt.Sprintf("_nodes/%s/", cluster.Config.NodeType)
}

// Return the path to the nomination file for the cluster, in respect to the node type.
func (cluster *Cluster) NominationPath() string {
	return fmt.Sprintf("_cluster/%s/%s", cluster.Config.NodeType, Nominationfile)
}

// Return the path to the query nodes.
func (cluster *Cluster) NodeQueryPath() string {
	return fmt.Sprintf("_nodes/%s/", config.NodeTypeQuery)
}

// Return the path to the primary file for the cluster, in respect to the node type.
func (cluster *Cluster) PrimaryPath() string {
	return fmt.Sprintf("_cluster/%s/%s", cluster.Config.NodeType, PrimaryFile)
}

// Save the cluster configuration.
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

// Return the path to the cluster configuration file.
func ConfigPath() string {
	return "_cluster/config.json"
}
