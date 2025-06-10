package cluster

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
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

	LeaseDuration = 70 * time.Second
	LeaseFile     = "LEASE"
	PrimaryFile   = "PRIMARY"
)

type Cluster struct {
	Auth               *auth.Auth             `json:"-"`
	AccessKeyManager   *auth.AccessKeyManager `json:"-"`
	subscriptions      map[string][]EventHandler
	Config             *config.Config `json:"-"`
	eventsChannel      chan *EventMessage
	eventsManager      *EventsManager
	fileSystemMutex    *sync.Mutex
	Initialized        bool   `json:"-"`
	Id                 string `json:"id"`
	QueryPrimary       string `json:"-"`
	nodes              []*NodeIdentifier
	MembersRetrievedAt time.Time `json:"-"`
	mutex              *sync.Mutex
	node               *Node

	localFileSystem     *storage.FileSystem
	objectFileSystem    *storage.FileSystem
	networkFileSystem   *storage.FileSystem
	tieredFileSystem    *storage.FileSystem
	tmpFileSystem       *storage.FileSystem
	tmpTieredFileSystem *storage.FileSystem
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
	err := cluster.NetworkFS().MkdirAll("_cluster/", 0755)

	if err != nil {
		return err
	}

	err = cluster.NetworkFS().MkdirAll(cluster.NodePath(), 0755)

	if err != nil {
		return err
	}

	err = cluster.NetworkFS().MkdirAll(cluster.NodePath(), 0755)

	if err != nil {
		return err
	}

	if _, err := cluster.NetworkFS().Stat(cluster.LeasePath()); os.IsNotExist(err) {
		_, err = cluster.NetworkFS().Create(cluster.LeasePath())

		if err != nil {
			return err
		}
	}

	if _, err := cluster.NetworkFS().Stat(cluster.PrimaryPath()); os.IsNotExist(err) {
		_, err = cluster.NetworkFS().Create(cluster.PrimaryPath())

		if err != nil {
			return err
		}
	}

	return nil
}

// Create a new cluster instance.
func NewCluster(config *config.Config) (*Cluster, error) {
	cluster := &Cluster{
		Config:          config,
		eventsChannel:   make(chan *EventMessage, 1000),
		fileSystemMutex: &sync.Mutex{},
		mutex:           &sync.Mutex{},
		subscriptions:   map[string][]EventHandler{},
	}

	cluster.runEventLoop()

	return cluster, nil
}

// Add a member to the cluster.
func (cluster *Cluster) AddMember(id string, address string) error {
	cluster.GetMembers(false)

	cluster.mutex.Lock()

	defer func() {
		cluster.mutex.Unlock()
		cluster.MembersRetrievedAt = time.Time{} // Clear the cache
	}()

	if slices.ContainsFunc(cluster.nodes, func(n *NodeIdentifier) bool {
		return n.Address == address
	}) {
		return nil
	}

	cluster.nodes = append(cluster.nodes, NewNodeIdentifier(address, id))

	return nil
}

// Get all the members of the cluster.
func (cluster *Cluster) GetMembers(cached bool) []*NodeIdentifier {
	cluster.mutex.Lock()
	defer cluster.mutex.Unlock()

	// Return the known nodes if the last retrieval was less than a minute
	if cached && time.Since(cluster.MembersRetrievedAt) < 1*time.Minute {
		return cluster.nodes
	}

	_, err := cluster.NetworkFS().Stat(cluster.NodePath())

	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}

		slog.Error("Error reading cluster nodes", "error", err, "path", cluster.NodePath())
		return nil
	}

	// Read the directory
	files, err := cluster.NetworkFS().ReadDir(cluster.NodePath())

	if err != nil {
		slog.Error("Error reading query nodes", "error", err, "path", cluster.NodePath())
		return nil
	}

	cluster.nodes = []*NodeIdentifier{}

	// Loop through the files and store the node addresses
	for _, file := range files {
		address := strings.ReplaceAll(file.Name(), "_", ":")
		hash := sha256.Sum256([]byte(address))
		ID := fmt.Sprintf("%d", binary.BigEndian.Uint64(hash[:]))
		cluster.nodes = append(cluster.nodes, NewNodeIdentifier(address, ID))
	}

	cluster.MembersRetrievedAt = time.Now()

	return cluster.nodes
}

// Get all the members of the cluster since a certain time.
func (cluster *Cluster) GetMembersSince(after time.Time) []*NodeIdentifier {
	if cluster.MembersRetrievedAt.After(after) {
		return cluster.GetMembers(true)
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
			log.Println("Error getting cluster ID from environment: ", err)
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
			log.Println("Error unmarshalling cluster configuration: ", err, string(clusterFile))
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
func (cluster *Cluster) IsMember(address string, since time.Time) bool {
	cluster.GetMembersSince(since)

	return slices.ContainsFunc(cluster.GetMembers(false), func(n *NodeIdentifier) bool {
		return n.Address == address
	})
}

func (cluster *Cluster) IsSingleNodeCluster() bool {
	cluster.GetMembers(true)

	return len(cluster.nodes) == 1 && cluster.nodes[0].Address == cluster.node.address
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

	return c.nodes
}

func (c *Cluster) NodeByID(id string) *NodeIdentifier {
	for _, node := range c.Nodes() {
		if node.ID == id {
			return node
		}
	}

	return nil
}

func (c *Cluster) OtherNodes() []*NodeIdentifier {
	nodes := []*NodeIdentifier{}
	address, _ := c.node.Address()

	c.GetMembers(true)

	for _, node := range c.nodes {
		if node.Address == address {
			continue
		}

		nodes = append(nodes, node)
	}

	return nodes
}

// Remove a member from the cluster.
func (cluster *Cluster) RemoveMember(address string, removeHardState bool) error {
	cluster.GetMembers(true)

	// Clear the cache
	cluster.MembersRetrievedAt = time.Time{}

	if cluster.node.primaryAddress == address {
		cluster.node.PrimaryHeartbeat = time.Time{}
		cluster.node.heartbeat()
	}

	for _, member := range cluster.Nodes() {
		if member.Address == address {
			cluster.nodes = slices.DeleteFunc(cluster.nodes, func(node *NodeIdentifier) bool {
				return node.Address == address
			})

			if removeHardState {
				// Remove the node address file
				err := cluster.NetworkFS().Remove(cluster.NodePath() + strings.ReplaceAll(address, ":", "_"))

				if err != nil {
					return err
				}
			}

			break
		}
	}

	return nil
}

// Return the path to the lease file for the cluster, in respect to the node type.
func (cluster *Cluster) LeasePath() string {
	return fmt.Sprintf("_cluster/%s", LeaseFile)
}

// Return the path to the current node in repsect to the node type.
func (cluster *Cluster) NodePath() string {
	return "_nodes/"
}

// Return the path to the primary file for the cluster, in respect to the node type.
func (cluster *Cluster) PrimaryPath() string {
	return fmt.Sprintf("_cluster/%s", PrimaryFile)
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

	return nil
}

// Return the path to the cluster configuration file.
func ConfigPath() string {
	return "_cluster/config.json"
}
