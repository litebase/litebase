package node

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"litebase/internal/config"
	"litebase/server/auth"
	"litebase/server/cluster"
	"litebase/server/storage"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var NodeIpAddress string
var NodeIPv6Address string
var NodeInstanceSingleton *NodeInstance
var NodeInstanceSingletonMutex sync.Mutex

const (
	NODE_HEARTBEAT_INTERVAL = 1 * time.Second
	NODE_HEARTBEAT_TIMEOUT  = 1 * time.Second
	NODE_IDLE_TIMEOUT       = 60 * time.Second
	NODE_STATE_ACTIVE       = "active"
	NODE_STATE_IDLE         = "idle"
)

type NodeInstance struct {
	address                 string
	cancel                  context.CancelFunc
	context                 context.Context
	databaseCheckpointer    NodeReplicaCheckpointer
	databaseWalSynchronizer NodeDatabaseWalSynchronizer
	joinedClusterAt         time.Time
	lastActive              time.Time
	Id                      string
	LeaseExpiresAt          int64
	LeaseRenewedAt          time.Time
	Membership              string
	mutex                   *sync.Mutex
	primaryAddress          string
	primary                 *NodePrimary
	PrimaryHeartbeat        time.Time
	replica                 *NodeReplica
	queryBuilder            NodeQueryBuilder
	requestTicker           *time.Ticker
	State                   string
	standBy                 chan struct{}
}

func Node() *NodeInstance {
	NodeInstanceSingletonMutex.Lock()
	defer NodeInstanceSingletonMutex.Unlock()

	if NodeInstanceSingleton == nil {
		NodeInstanceSingleton = &NodeInstance{
			address:    "",
			lastActive: time.Time{},
			Membership: cluster.CLUSTER_MEMBERSHIP_STAND_BY,
			// Membership: cluster.CLUSTER_MEMBERSHIP_REPLICA,
			mutex:   &sync.Mutex{},
			standBy: make(chan struct{}),
			State:   NODE_STATE_ACTIVE,
		}

		hash := sha256.Sum256([]byte(NodeInstanceSingleton.Address()))
		NodeInstanceSingleton.Id = hex.EncodeToString(hash[:])
		NodeInstanceSingleton.context, NodeInstanceSingleton.cancel = context.WithCancel(context.Background())
	}

	return NodeInstanceSingleton
}

func (n *NodeInstance) Address() string {
	if n.address != "" {
		return n.address
	}

	address, err := os.Hostname()

	if err != nil {
		log.Fatal(err)
	}

	n.address = fmt.Sprintf("%s:%s", address, config.Get().Port)

	return n.address

}

func (n *NodeInstance) AddressPath() string {
	// Replace the colon in the address with an underscore
	address := strings.ReplaceAll(n.Address(), ":", "_")

	return fmt.Sprintf("%s%s", cluster.NodePath(), address)
}

func (n *NodeInstance) Context() context.Context {
	return n.context
}

func (n *NodeInstance) Heartbeat() {
	if n.IsPrimary() {
		if cluster.LEASE_DURATION-time.Since(n.LeaseRenewedAt) < 10*time.Second {
			n.renewLease()
		} else {
			n.Primary().Heartbeat()
		}

		return
	}

	if n.context.Err() != nil {
		return
	}

	if !n.IsStandBy() && !n.primaryLeaseVerification() {
		success := n.runElection()

		if !success {
			time.Sleep(cluster.ELECTION_RETRY_WAIT)
		}
	}
}

func (n *NodeInstance) IsIdle() bool {
	return n.State == NODE_STATE_IDLE
}

func (n *NodeInstance) IsPrimary() bool {
	// If the node has not been activatedf, tick it before running these checks
	if n.lastActive.IsZero() {
		n.Tick()
	}

	if n.Membership == cluster.CLUSTER_MEMBERSHIP_REPLICA || n.Membership == cluster.CLUSTER_MEMBERSHIP_STAND_BY {
		return false
	}

	// If the cluster membership is primary and the lease is still valid
	if n.Membership == cluster.CLUSTER_MEMBERSHIP_PRIMARY && time.Now().Unix() < n.LeaseExpiresAt {
		return true
	}

	return n.primaryFileVerification()
}

func (n *NodeInstance) IsReplica() bool {
	return n.Membership == cluster.CLUSTER_MEMBERSHIP_REPLICA
}

func (n *NodeInstance) IsStandBy() bool {
	return n.Membership == cluster.CLUSTER_MEMBERSHIP_STAND_BY
}

func (n *NodeInstance) joinCluster() error {
	if err := n.storeAddress(); err != nil {
		return err
	}

	err := cluster.Get().AddMember(config.Get().NodeType, n.Address())

	if err != nil {
		return err
	}

	return nil
}

func (n *NodeInstance) monitorPrimary() {
	n.Heartbeat()

	ticker := time.NewTicker(NODE_HEARTBEAT_INTERVAL)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if n.IsIdle() {
				continue
			}

			n.Heartbeat()
		case <-n.context.Done():
			ticker.Stop()
			// Exit if the parent context is canceled
			return
		}
	}
}

func (n *NodeInstance) Primary() *NodePrimary {
	return n.primary
}

func (n *NodeInstance) PrimaryAddress() string {
	if n.primaryAddress == "" {
		primaryData, err := storage.ObjectFS().ReadFile(cluster.PrimaryPath())

		if err != nil {
			log.Printf("Failed to read primary file: %v", err)
			return ""
		}

		n.primaryAddress = string(primaryData)
	}

	return n.primaryAddress
}

func (n *NodeInstance) primaryLeaseVerification() bool {
	if n.IsReplica() && !n.PrimaryHeartbeat.IsZero() && time.Since(n.PrimaryHeartbeat) < 3*time.Second {
		return true
	}

	primaryData, err := storage.ObjectFS().ReadFile(cluster.PrimaryPath())

	if err != nil {
		log.Printf("Failed to read primary file: %v", err)
		return false
	}

	// There is a primary file but it is empty
	if len(primaryData) == 0 {
		return false
	}

	// Check if the primary is still alive
	leaseData, err := storage.ObjectFS().ReadFile(cluster.LeasePath())

	if err != nil {
		log.Printf("Failed to read lease file: %v", err)
		return false
	}

	if len(leaseData) == 0 {
		return false
	}

	leaseTime, err := strconv.ParseInt(string(leaseData), 10, 64)

	if err != nil {
		log.Printf("Failed to parse lease timestamp: %v", err)
		return false
	}

	if time.Now().Unix() >= leaseTime {
		n.removePrimaryStatus()
		n.SetMembership(cluster.CLUSTER_MEMBERSHIP_REPLICA)

		return false
	}

	return true
}

func (n *NodeInstance) primaryFileVerification() bool {
	// Check if the primary file exists and is not empty
	if primaryData, err := storage.ObjectFS().ReadFile(cluster.PrimaryPath()); err != nil || len(primaryData) == 0 || string(primaryData) != n.Address() {
		if err != nil {
			log.Printf("Error accessing primary file: %v", err)
		}

		return false
	}

	// Check if the lease file exists, is not empty, and has a valid future timestamp
	leaseData, err := storage.ObjectFS().ReadFile(cluster.LeasePath())

	if err != nil || len(leaseData) == 0 {
		return false
	}

	// Check if the lease file has a valid future timestamp
	leaseTime, err := strconv.ParseInt(string(leaseData), 10, 64)

	if err != nil {
		log.Printf("Failed to parse lease timestamp: %v", err)
		return false
	}

	if time.Now().Unix() < leaseTime {
		return true
	}

	return false
}

// Release the lease and remove the primary status from the node. This should
// be called before changing the cluster membership to replica.
func (n *NodeInstance) releaseLease() error {
	n.LeaseExpiresAt = 0

	if n.Membership != cluster.CLUSTER_MEMBERSHIP_PRIMARY {
		return fmt.Errorf("node is not a leader")
	}

	// Refactor to directly truncate files without checking for existence
	if err := truncateFile(cluster.PrimaryPath()); err != nil {
		return err
	}

	if err := truncateFile(cluster.LeasePath()); err != nil {
		return err
	}

	return nil
}

func (n *NodeInstance) Replica() *NodeReplica {
	return n.replica
}

func (n *NodeInstance) removeAddress() error {
	return storage.ObjectFS().Remove(n.AddressPath())
}

func (n *NodeInstance) removePrimaryStatus() error {
	// Release the lease
	n.releaseLease()

	if n.primary != nil {
		n.primary.Stop()
		n.primary = nil
	}

	return nil
}

func (n *NodeInstance) renewLease() error {
	if n.Membership != cluster.CLUSTER_MEMBERSHIP_PRIMARY {
		return fmt.Errorf("node is not a leader")
	}

	if err := n.context.Err(); err != nil {
		log.Println("Operation canceled before starting.")
		return err
	}

	// Verify the primary is stil the current node
	primaryAddress, err := storage.ObjectFS().ReadFile(cluster.PrimaryPath())

	if err != nil {
		return err
	}

	if string(primaryAddress) != n.Address() {
		n.SetMembership(cluster.CLUSTER_MEMBERSHIP_REPLICA)

		return fmt.Errorf("primary address verification failed")
	}

	if err := n.context.Err(); err != nil {
		log.Println("Operation canceled before starting.")
		return err
	}

	expiresAt := time.Now().Add(cluster.LEASE_DURATION).Unix()
	leaseTimestamp := strconv.FormatInt(expiresAt, 10)

	err = storage.ObjectFS().WriteFile(cluster.LeasePath(), []byte(leaseTimestamp), os.ModePerm)

	if err != nil {
		log.Printf("Failed to write lease file: %v", err)
		return err
	}

	if err := n.context.Err(); err != nil {
		log.Println("Operation canceled before starting.")
		return err
	}

	// Verify the Lease file has the written value
	leaseData, err := storage.ObjectFS().ReadFile(cluster.LeasePath())

	if err != nil {
		log.Printf("Failed to read lease file: %v", err)
		return err
	}

	if string(leaseData) != leaseTimestamp {
		return fmt.Errorf("failed to verify lease file")
	}

	n.LeaseRenewedAt = time.Now()
	n.LeaseExpiresAt = expiresAt

	return nil
}

func (n *NodeInstance) runElection() bool {
	if n.context.Err() != nil {
		log.Println("Operation canceled before starting.")
		return false
	}

	// Attempt to open the nomination file with exclusive lock
	nominationFile, err := storage.ObjectFS().OpenFile(cluster.NominationPath(), os.O_RDWR|os.O_CREATE, 0644)

	if err != nil {
		log.Printf("Failed to open nomination file: %v", err)
		return false
	}

	defer nominationFile.Close()

	// TODO: Refactor this for the new storage system
	// Attempt to acquire an exclusive lock
	// err = syscall.Flock(int(nominationFile.Fd()), syscall.LOCK_EX)

	// if err != nil {
	// 	log.Printf("Failed to lock nomination file: %v", err)
	// 	return false
	// }

	// Todo: Refactor this for the new storage system
	// defer syscall.Flock(int(nominationFile.Fd()), syscall.LOCK_UN) // Ensure unlock

	// if n.context.Err() != nil {
	// 	log.Println("Operation canceled before starting.")
	// 	return false
	// }

	// Check if the nomination file is empty or contains this node's address
	nominationData, err := io.ReadAll(nominationFile)

	if err != nil {
		log.Printf("Failed to read nomination file: %v", err)
		return false
	}

	address := n.Address()
	timestamp := time.Now().UnixNano()
	entry := fmt.Sprintf("%s,%d\n", address, timestamp)

	if len(nominationData) == 0 || !strings.Contains(string(nominationData), address) {
		nominationFile.Seek(0, io.SeekStart)

		err := nominationFile.Truncate(0)

		if err != nil {
			log.Printf("Failed to truncate nomination file: %v", err)
			return false
		}

		_, err = nominationFile.WriteString(entry)

		if err != nil {
			log.Printf("Failed to write to nomination file: %v", err)
			return false
		}
	}
	// else {
	// File is not empty and does not contain this node's address
	// Implement logic to determine if this node should still become primary based on timestamps or other criteria
	// }

	// Logic to determine if this node becomes primary based on the contents of the nomination file
	// This could involve reading back the file contents and checking timestamps or other coordination logic

	if n.context.Err() != nil {
		log.Println("Operation canceled before starting.")
		return false
	}

	// Assuming this node is determined to be primary
	if isPrimaryBasedOnFileContents(nominationData, address, timestamp) {
		err = storage.ObjectFS().WriteFile(cluster.PrimaryPath(), []byte(address), 0644)

		if err != nil {
			log.Printf("Failed to write primary file: %v", err)
			return false
		}

		n.SetMembership(cluster.CLUSTER_MEMBERSHIP_PRIMARY)
		truncateFile(cluster.NominationPath())

		err := n.renewLease()

		if err != nil {
			log.Printf("Failed to renew lease: %v", err)
			return false
		}

		return true
	}

	return false
}

func (n *NodeInstance) runTicker() {
	n.requestTicker = time.NewTicker(1 * time.Second)

	for {
		select {
		case <-n.context.Done():
			return
		case <-n.requestTicker.C:
			// Continue if the node is idle
			if n.State == NODE_STATE_IDLE {
				continue
			}

			// Ensure Replicas are still connected to the primary
			// if n.Membership == cluster.CLUSTER_MEMBERSHIP_REPLICA && n.primaryConnection == nil && !n.connecting {
			// 	n.connectWithPrimary()
			// }

			// Continue if the node has not been inactive for the idle timeout duration
			if n.lastActive == (time.Time{}) || time.Since(n.lastActive) <= NODE_IDLE_TIMEOUT {
				continue
			}

			// Change the cluster membership to stand by
			// n.SetMembership(cluster.CLUSTER_MEMBERSHIP_STAND_BY)
		}
	}
}

func (n *NodeInstance) SetDatabaseCheckpointer(checkpointer NodeReplicaCheckpointer) {
	n.databaseCheckpointer = checkpointer
}

func (n *NodeInstance) SetDatabaseWalSchronizer(synchronizer NodeDatabaseWalSynchronizer) {
	n.databaseWalSynchronizer = synchronizer
}

func (n *NodeInstance) SetMembership(membership string) {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	prevMembership := n.Membership

	n.Membership = membership
	// Forget the last known primary address
	n.primaryAddress = ""

	if membership == cluster.CLUSTER_MEMBERSHIP_PRIMARY {
		n.primary = NewNodePrimary(n.queryBuilder)

		if n.replica != nil {
			n.replica.Stop()
			n.replica = nil
		}
	}

	if membership == cluster.CLUSTER_MEMBERSHIP_REPLICA && prevMembership != cluster.CLUSTER_MEMBERSHIP_PRIMARY {
		n.replica = NewNodeReplica(n)

		if n.primary != nil {
			n.removePrimaryStatus()
		}
	}

	if membership == cluster.CLUSTER_MEMBERSHIP_STAND_BY {
		n.State = NODE_STATE_IDLE
	}
}

func (n *NodeInstance) SetQueryBuilder(queryBuilder NodeQueryBuilder) {
	n.queryBuilder = queryBuilder
}

func (n *NodeInstance) Shutdown() error {
	NodeInstanceSingletonMutex.Lock()
	defer NodeInstanceSingletonMutex.Unlock()

	if n.IsReplica() {
		n.replica.LeaveCluster()
	}

	err := n.removeAddress()

	if err != nil {
		log.Println(err)
	}

	if n.IsPrimary() {
		n.removePrimaryStatus()
	}

	n.cancel()

	NodeInstanceSingleton = nil

	return nil
}

func (n *NodeInstance) Start() error {
	if err := n.joinCluster(); err != nil {
		log.Println(err)
		return err
	}

	go n.monitorPrimary()
	go n.runTicker()

	n.Tick()

	return nil
}

func (n *NodeInstance) storeAddress() error {
	return storage.ObjectFS().WriteFile(n.AddressPath(), []byte(n.Address()), 0644)
}

func (n *NodeInstance) Tick() {
	// Check if the is still registered as primary
	if n.lastActive.IsZero() {
		if n.primaryFileVerification() {
			n.SetMembership(cluster.CLUSTER_MEMBERSHIP_PRIMARY)
		}
	}

	// Check if the node has be joined the cluster
	if n.PrimaryAddress() != "" && n.PrimaryAddress() != n.Address() && n.replica != nil && n.joinedClusterAt.IsZero() {
		err := n.replica.JoinCluster()

		if err != nil {
			log.Println(err)
		} else {
			n.joinedClusterAt = time.Now()
		}
	}

	n.lastActive = time.Now()

	if n.State == NODE_STATE_IDLE {
		n.State = NODE_STATE_ACTIVE
	}

	// If the node is a standby, and it hasn't won the election at this point,
	// it should manually become a replica and ensure it has membership.
	if n.Membership == cluster.CLUSTER_MEMBERSHIP_STAND_BY {
		n.SetMembership(cluster.CLUSTER_MEMBERSHIP_REPLICA)
		// Join the cluster as a replica

		n.Heartbeat()
	}
}

// isPrimaryBasedOnFileContents checks if the current node is the primary based on the contents of the nomination file.
// nominationData is the content of the nomination file.
// address is the address of the current node().
// timestamp is the timestamp when the current node attempted to nominate itself.
func isPrimaryBasedOnFileContents(nominationData []byte, address string, timestamp int64) bool {
	// Split the file content into lines, each representing an entry
	lines := strings.Split(string(nominationData), "\n")

	for _, line := range lines {
		if line == "" {
			continue // Skip empty lines
		}

		parts := strings.Split(line, ",")

		if len(parts) != 2 {
			log.Printf("Invalid entry in nomination file: %s", line)
			continue
		}

		entryAddress := parts[0]

		entryTimestamp, err := strconv.ParseInt(parts[1], 10, 64)

		if err != nil {
			log.Printf("Invalid timestamp for entry in nomination file: %s", parts[1])
			continue
		}

		// If there's an entry with an earlier timestamp, current node cannot be primary
		if entryTimestamp < timestamp && entryAddress != address {
			return false
		}
	}

	// If no entry has an earlier timestamp, or in case of a tie, the node with the lexicographically smaller address wins
	return true
}

// truncateFile truncates the specified file. It creates the file if it does not exist.
func truncateFile(filePath string) error {
	return storage.ObjectFS().WriteFile(filePath, []byte(""), os.ModePerm)
}

func FilePath(ipAddress string) string {
	return fmt.Sprintf("%s%s", cluster.NodePath(), ipAddress)
}

func Has(ip string) bool {
	nodes := OtherNodes()

	log.Println("NODES", nodes)

	for _, node := range nodes {
		if node.String() == ip {
			return true
		}
	}

	return false
}

func Init(queryBuilder NodeQueryBuilder, checkPointer NodeReplicaCheckpointer, walSynchronizer NodeDatabaseWalSynchronizer) {
	registerNodeMessages()

	// Make directory if it doesn't exist
	if storage.ObjectFS().Stat(cluster.NodePath()); os.IsNotExist(os.ErrNotExist) {
		storage.ObjectFS().Mkdir(cluster.NodePath(), 0755)
	}

	Node().SetQueryBuilder(queryBuilder)
	Node().SetDatabaseCheckpointer(checkPointer)
	Node().SetDatabaseWalSchronizer(walSynchronizer)
}

func OtherNodes() []*NodeIdentifier {
	nodes := []*NodeIdentifier{}
	address := Node().Address()
	cluster.Get().GetMembers(true)

	for _, node := range cluster.Get().QueryNodes {
		if node != address {
			nodes = append(nodes, &NodeIdentifier{
				Address: strings.Split(node, ":")[0],
				Port:    strings.Split(node, ":")[1],
			})
		}
	}

	for _, node := range cluster.Get().StorageNodes {
		if node != address {
			nodes = append(nodes, &NodeIdentifier{
				Address: strings.Split(node, ":")[0],
				Port:    strings.Split(node, ":")[1],
			})
		}
	}

	return nodes
}

func OtherQueryNodes() []*NodeIdentifier {
	nodes := []*NodeIdentifier{}
	address := Node().Address()

	for _, node := range cluster.Get().QueryNodes {
		if node != address {
			nodes = append(nodes, &NodeIdentifier{
				Address: strings.Split(node, ":")[0],
				Port:    strings.Split(node, ":")[1],
			})
		}
	}

	return nodes
}

func OtherStorageNodes() []*NodeIdentifier {
	nodes := []*NodeIdentifier{}
	address := Node().Address()

	for _, node := range cluster.Get().StorageNodes {
		if node != address {
			nodes = append(nodes, &NodeIdentifier{
				Address: strings.Split(node, ":")[0],
				Port:    strings.Split(node, ":")[1],
			})
		}
	}

	return nodes
}

func (n *NodeInstance) Publish(nodeMessage NodeMessage) error {
	return n.primary.Publish(nodeMessage)
}

func PurgeDatabaseSettings(databaseUuid string) {
	nodes := OtherNodes()

	for _, node := range nodes {
		go func(node *NodeIdentifier) {
			url := fmt.Sprintf("http://%s:%s/databases/%s/settings/purge", node.Address, node.Port, databaseUuid)
			req, err := http.NewRequest("POST", url, nil)

			if err != nil {
				log.Println(err)
				return
			}

			client := &http.Client{}

			res, err := client.Do(req)

			if err != nil {
				log.Println(err)
				return
			}

			defer res.Body.Close()

			if res.StatusCode != 200 {
				log.Println(res)
			}
		}(node)
	}
}

func SendEvent(node *NodeIdentifier, message NodeEvent) {
	url := fmt.Sprintf("http://%s:%s/events", node.Address, node.Port)
	data, err := json.Marshal(message)

	if err != nil {
		log.Println(err)
		return
	}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(data))

	if err != nil {
		log.Println(err)
		return
	}

	encryptedHeader, err := auth.SecretsManager().Encrypt(
		config.Get().Signature,
		Node().Address(),
	)

	if err != nil {
		log.Println(err)
		return
	}

	req.Header.Set("X-Lbdb-Node", encryptedHeader)

	client := &http.Client{}

	res, err := client.Do(req)

	if err != nil {
		log.Println(err)
		return
	}

	defer res.Body.Close()

	if res.StatusCode != 200 {
		log.Println(res)
	}
}

func (n *NodeInstance) Send(nodeMessage NodeMessage) (NodeMessage, error) {
	return n.replica.Send(nodeMessage)
}

func (n *NodeInstance) SendWithStreamingResonse(nodeMessage NodeMessage) (chan NodeMessage, error) {
	return n.replica.SendWithStreamingResonse(nodeMessage)

}
