package cluster

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/litebase/litebase/server/cluster/messages"
	"github.com/litebase/litebase/server/storage"
)

const (
	NodeHeartbeatInterval = 1 * time.Second
	NodeIdleTimeout       = 60 * time.Second
	NodeStateActive       = "active"
	NodeStateIdle         = "idle"
)

var addressProvider func() string

type Node struct {
	address           string
	cancel            context.CancelFunc
	Cluster           *Cluster
	context           context.Context
	Election          *ClusterElection
	Elections         []*ClusterElection
	Initialized       bool
	joinedClusterAt   time.Time
	lease             *Lease
	LastActive        time.Time
	ID                string
	Membership        string
	mutex             *sync.Mutex
	primaryAddress    string
	primary           *NodePrimary
	PrimaryHeartbeat  time.Time
	queryBuilder      NodeQueryBuilder
	queryResponsePool NodeQueryResponsePool
	replica           *NodeReplica
	requestTicker     *time.Ticker
	started           chan bool
	State             string
	startedAt         time.Time
	storedAddressAt   time.Time
	walSynchronizer   NodeWalSynchronizer
}

// Create a new instance of a node.
func NewNode(cluster *Cluster) *Node {
	node := &Node{
		address:    "",
		Cluster:    cluster,
		LastActive: time.Time{},
		Membership: ClusterMembershipReplica,
		mutex:      &sync.Mutex{},
		started:    make(chan bool, 1),
		State:      NodeStateActive,
	}

	address, err := node.Address()

	if err != nil {
		slog.Debug("Failed to get address", "error", err)
		return nil
	}

	hash := sha256.Sum256([]byte(address))
	node.ID = fmt.Sprintf("%d", binary.BigEndian.Uint64(hash[:]))
	node.context, node.cancel = context.WithCancel(context.Background())

	return node
}

// Get the address of the node.
func (n *Node) Address() (string, error) {
	if n.address != "" {
		return n.address, nil
	}

	var address string
	var err error

	if addressProvider != nil {
		address = addressProvider()
	} else if n.Cluster.Config.NodeAddressProvider != "" {
		addressProviderFunc := nodeAddressProviders[NodeAddressProviderKey(n.Cluster.Config.NodeAddressProvider)]

		if addressProviderFunc != nil {
			address, err = addressProviderFunc()

			if err != nil {
				slog.Debug("Failed to get address from provider", "error", err)
				return "", err
			}
		} else {
			address = "127.0.0.1"
		}
	} else {
		address = "127.0.0.1"
	}

	n.address = fmt.Sprintf("%s:%s", address, n.Cluster.Config.Port)

	return n.address, nil

}

// Return the path for where the address will be stored.
func (n *Node) AddressPath() string {
	// Replace the colon in the address with an underscore
	address, _ := n.Address()

	address = strings.ReplaceAll(address, ":", "_")

	return fmt.Sprintf("%s%s", n.Cluster.NodePath(), address)
}

func (n *Node) AddPeerElection(election *ClusterElection) {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	n.Elections = append(n.Elections, election)
}

// Return the context for the node.
func (n *Node) Context() context.Context {
	return n.context
}

// Check if the node address is stored and if not, store it.
func (n *Node) ensureNodeAddressStored() error {
	if n.storedAddressAt.IsZero() || time.Since(n.storedAddressAt) > 1*time.Minute {
		// Check if the address is already stored
		if _, err := n.Cluster.NetworkFS().Stat(n.AddressPath()); err == nil {
			n.storedAddressAt = time.Now()
			return nil
		}

		// If the address is not stored, the node needs to rejoin the cluster
		n.joinedClusterAt = time.Time{}

		err := n.JoinCluster()

		if err != nil {
			slog.Error("Failed to join cluster", "error", err)
			return err
		}
	}

	return nil
}

func (n *Node) HasPeerElectionRunning() bool {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	n.purgeExpiredElections()

	return len(n.Elections) > 0
}

// Trigger the node to perform a heartbeat.
func (n *Node) heartbeat() {
	n.mutex.Lock()

	n.ensureNodeAddressStored()

	if n.Membership == ClusterMembershipPrimary {
		n.mutex.Unlock()

		if n.Lease() == nil {
			slog.Error("No lease found for primary node, cannot send heartbeat")
			n.removePrimaryStatus()
			return
		}

		if n.Lease().ShouldRenew() {
			n.Lease().Renew()
		} else {
			if n.Primary() == nil {
				return
			}

			err := n.Primary().Heartbeat()

			if err != nil {
				slog.Debug("Failed to send heartbeat", "error", err)
			}
		}

		return
	} else {
		n.mutex.Unlock()
	}

	if n.context.Err() != nil {
		return
	}

	if !n.primaryLeaseVerification() {
		success, err := n.runElection()

		if err != nil {
			slog.Debug("Failed to run election", "error", err)
		}

		if !success {
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// Initialize the node with the query builder and wal synchronizer.
func (n *Node) Init(
	queryBuilder NodeQueryBuilder,
	queryResponsePool NodeQueryResponsePool,
	walSynchronizer NodeWalSynchronizer,
) {
	registerNodeMessages()

	// Make directory if it doesn't exist
	if n.Cluster.NetworkFS().Stat(n.Cluster.NodePath()); os.IsNotExist(os.ErrNotExist) {
		n.Cluster.NetworkFS().Mkdir(n.Cluster.NodePath(), 0755)
	}

	n.SetQueryBuilder(queryBuilder)
	n.SetQueryResponsePool(queryResponsePool)
	n.SetWALSynchronizer(walSynchronizer)
	// n.SetRangeSynchronizer(rangeSynchronizer)

	n.Initialized = true
}

func (n *Node) IsIdle() bool {
	return n.State == NodeStateIdle
}

func (n *Node) IsPrimary() bool {
	// If an election is running, wait for it to finish
	if n.Election != nil && n.Election.Running() {
		select {
		case <-n.Election.Context().Done():
		default:
			break
		}
	}

	// If the node has not been activated, tick it before running these checks
	if n.LastActive.IsZero() || time.Since(n.LastActive) > 5*time.Minute {
		n.Tick()
	}

	if n.Membership == ClusterMembershipReplica {
		return false
	}

	// If the cluster membership is primary and the lease is still valid
	if n.Membership == ClusterMembershipPrimary &&
		n.Lease() != nil &&
		n.Lease().IsUpToDate() {
		return true
	}

	return n.primaryFileVerification()
}

func (n *Node) IsReplica() bool {
	// If an election is running, wait for it to finish
	if n.Election != nil && n.Election.Running() {
		select {
		case <-n.Election.Context().Done():
		default:
			break
		}
	}

	return n.Membership == ClusterMembershipReplica && n.replica != nil
}

func (n *Node) JoinCluster() error {
	if !n.joinedClusterAt.IsZero() {
		return nil
	}

	if n.storedAddressAt.IsZero() {
		if err := n.StoreAddress(); err != nil {
			return err
		}
	}

	address, err := n.Address()

	if err != nil {
		slog.Debug("Failed to get node address", "error", err)
		return err
	}

	// Check if the node has joined the cluster
	if n.PrimaryAddress() != "" && n.PrimaryAddress() != address && n.replica != nil && n.joinedClusterAt.IsZero() {
		err := n.replica.JoinCluster()

		if err != nil {
			slog.Debug("Failed to join cluster", "error", err)
		} else {
			n.joinedClusterAt = time.Now()
		}
	} else {
		n.joinedClusterAt = time.Now()
	}

	err = n.Cluster.Broadcast("cluster:join", map[string]string{
		"address": address,
		"ID":      n.ID,
	})

	if err != nil {
		slog.Debug("Failed to broadcast join message", "error", err)
		return err
	}

	return nil
}

// Return the lease of the node
func (n *Node) Lease() *Lease {
	return n.lease
}

// Monitor the primary node and perform heartbeat checks at regular intervals.
func (n *Node) monitorPrimary() {
	ticker := time.NewTicker(NodeHeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if n.IsIdle() {
				continue
			}

			if n.context.Err() != nil {
				return
			}

			n.heartbeat()

		case <-n.context.Done():
			return
		}
	}
}

func (n *Node) PeerElections() []*ClusterElection {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	n.purgeExpiredElections()

	return n.Elections
}

func (n *Node) Primary() *NodePrimary {
	return n.primary
}

func (n *Node) PrimaryAddress() string {
	if n.primaryAddress == "" {
		primaryData, err := n.Cluster.NetworkFS().ReadFile(n.Cluster.PrimaryPath())

		if err != nil {
			return ""
		}

		n.primaryAddress = string(primaryData)
	}

	return n.primaryAddress
}

func (n *Node) primaryLeaseVerification() bool {
	if n.IsReplica() && !n.PrimaryHeartbeat.IsZero() && time.Since(n.PrimaryHeartbeat) < 3*time.Second {
		return true
	}

	primaryData, err := n.Cluster.NetworkFS().ReadFile(n.Cluster.PrimaryPath())

	if err != nil {
		slog.Error("Failed to read primary file", "error", err)
		return false
	}

	// There is a primary file but it is empty
	if len(primaryData) == 0 {
		return false
	}

	// Check if the primary is still alive
	leaseData, err := n.Cluster.NetworkFS().ReadFile(n.Cluster.LeasePath())

	if err != nil && !os.IsNotExist(err) {
		slog.Error("Failed to read lease file", "error", err)
		return false
	}

	if len(leaseData) == 0 {
		return false
	}

	leaseTime, err := strconv.ParseInt(string(leaseData), 10, 64)

	if err != nil {
		slog.Error("Failed to parse lease timestamp", "error", err)
		return false
	}

	if time.Now().Unix() >= leaseTime {
		n.removePrimaryStatus()
		n.SetMembership(ClusterMembershipReplica)

		return false
	}

	return true
}

func (n *Node) primaryFileVerification() bool {
	address, _ := n.Address()

	// Check if the primary file exists and is not empty
	if primaryData, err := n.Cluster.NetworkFS().ReadFile(n.Cluster.PrimaryPath()); err != nil || len(primaryData) == 0 || string(primaryData) != address {
		if err != nil && !os.IsNotExist(err) {
			slog.Error("Error accessing primary file", "error", err)
		}

		return false
	}

	// Check if the lease file exists, is not empty, and has a valid future timestamp
	leaseData, err := n.Cluster.NetworkFS().ReadFile(n.Cluster.LeasePath())

	if err != nil || len(leaseData) == 0 {
		return false
	}

	// Check if the lease file has a valid future timestamp
	leaseTime, err := strconv.ParseInt(string(leaseData), 10, 64)

	if err != nil {
		slog.Error("Failed to parse lease timestamp", "error", err)
		return false
	}

	if time.Now().Unix() < leaseTime {
		return true
	}

	return false
}

func (n *Node) purgeExpiredElections() {
	n.Elections = slices.DeleteFunc(n.Elections, func(e *ClusterElection) bool {
		return e.Expired()
	})
}

func (n *Node) removePrimaryStatus() error {
	if n.primary != nil {
		n.primary = nil
	}

	if n.Lease() == nil {
		return nil
	}

	// Release the lease
	err := n.Lease().Release()

	if err != nil {
		slog.Error("Failed to release lease", "error", err)
		return err
	}

	n.lease = nil

	return nil
}

func (n *Node) Replica() *NodeReplica {
	return n.replica
}

// Return the query builder of the node.
func (n *Node) QueryBuilder() NodeQueryBuilder {
	return n.queryBuilder
}

func (n *Node) QueryResponsePool() NodeQueryResponsePool {
	return n.queryResponsePool
}

// Remove the address of the node from storage so it is no longer discoverable
// by other nodes in the cluster.
func (n *Node) removeAddress() error {
	return n.Cluster.NetworkFS().Remove(n.AddressPath())
}

// Run an election to determine the primary node in the cluster group.
func (n *Node) runElection() (bool, error) {
	if n.Election != nil && n.Election.Running() {
		return false, ErrElectionAlreadyRunning
	}

	defer func() {
		if n.Election.Stopped() {
			return
		}

		n.Election.Stop()
	}()

	n.mutex.Lock()

	if n.Election == nil || n.Election.Stopped() {
		n.Election = NewClusterElection(n)
	}

	n.mutex.Unlock()

	elected, err := n.Election.run()

	if err != nil {
		return false, err
	}

	if !elected {
		return elected, nil
	}

	n.SetMembership(ClusterMembershipPrimary)
	n.lease = NewLease(n)
	err = n.lease.Renew()

	if err != nil {
		return false, fmt.Errorf("failed to renew lease after election: %w", err)
	}

	return true, nil
}

// Run the node ticker to monitor the node state.
func (n *Node) runTicker() {
	n.requestTicker = time.NewTicker(1 * time.Second)

	for {
		select {
		case <-n.context.Done():
			return
		case <-n.requestTicker.C:
			// Continue if the node is idle
			if n.State == NodeStateIdle {
				continue
			}

			n.mutex.Lock()
			lastActive := n.LastActive

			// Continue if the node has not been inactive for the idle timeout duration
			if lastActive.IsZero() || time.Since(lastActive) <= NodeIdleTimeout {
				n.mutex.Unlock()
				continue
			}

			n.mutex.Unlock()

			n.Tick()
		}
	}
}

func (n *Node) Send(message messages.NodeMessage) (messages.NodeMessage, error) {
	return n.replica.Send(message)
}

func (n *Node) SendEvent(node *NodeIdentifier, message NodeEvent) error {
	// Check if the context is canceled
	if n.context.Err() != nil {
		return nil
	}

	url := fmt.Sprintf("http://%s/events", node.Address)

	data, err := json.Marshal(message)

	if err != nil {
		slog.Error("Failed to marshal event message", "error", err)
		return err
	}

	req, err := http.NewRequestWithContext(n.context, "POST", url, bytes.NewBuffer(data))

	if err != nil {
		slog.Error("Failed to create event request", "error", err)
		return err
	}

	if n.context.Err() != nil {
		return fmt.Errorf("operation canceled")
	}

	err = n.setInternalHeaders(req)

	if err != nil {
		return err
	}

	client := &http.Client{
		Timeout: 1 * time.Second,
	}

	res, err := client.Do(req)

	if err != nil {
		return err
	}

	if n.context.Err() != nil {
		return nil
	}

	defer res.Body.Close()

	if res.StatusCode >= 400 {
		return fmt.Errorf("failed to send message: %d", res.StatusCode)
	}

	return nil
}

func SetAddressProvider(provider func() string) {
	addressProvider = provider
}

func (n *Node) setInternalHeaders(req *http.Request) error {
	address, _ := n.Address()

	encryptedHeader, err := n.Cluster.Auth.SecretsManager.Encrypt(
		n.Cluster.Config.Signature,
		[]byte(address),
	)

	if err != nil {
		return err
	}

	req.Header.Set("X-Lbdb-Node", string(encryptedHeader))
	req.Header.Set("X-Lbdb-Node-Timestamp", fmt.Sprintf("%d", time.Now().UnixNano()))

	return nil
}

// Set the membership of the node in the cluster.
func (n *Node) SetMembership(membership string) {
	prevMembership := n.Membership
	n.Membership = membership

	if membership == ClusterMembershipPrimary {
		n.primary = NewNodePrimary(n)

		if n.replica != nil {
			n.replica.Stop()
			n.replica = nil
		}

		// Ensure the primary checks for dirty files that need to be synced from
		// tiered storage.
		if driver, ok := n.Cluster.TieredFS().Driver().(*storage.TieredFileSystemDriver); ok {
			driver.SyncDirtyFiles()
		}
	}

	if membership == ClusterMembershipReplica && prevMembership != ClusterMembershipPrimary && n.PrimaryAddress() != "" {
		n.replica = NewNodeReplica(n)
	}
}

// Set the query builder for the node.
func (n *Node) SetQueryBuilder(queryBuilder NodeQueryBuilder) {
	n.queryBuilder = queryBuilder
}

// Set the query response pool for the node.
func (n *Node) SetQueryResponsePool(queryResponsePool NodeQueryResponsePool) {
	n.queryResponsePool = queryResponsePool
}

// Set the WAL synchronizer for the node.
func (n *Node) SetWALSynchronizer(walSynchronizer NodeWalSynchronizer) {
	n.walSynchronizer = walSynchronizer
}

// Shutdown the node and perform necessary cleanup operations.
func (n *Node) Shutdown() error {
	if n.IsPrimary() {
		n.Primary().Shutdown()

		if n.Lease() != nil {
			n.Lease().Release()
		}
	}

	err := n.Cluster.Broadcast("cluster:leave", map[string]string{
		"address": n.address,
	})

	if err != nil {
		slog.Debug("Failed to broadcast leave message", "error", err)
	}

	err = n.removeAddress()

	if err != nil && !os.IsNotExist(err) {
		slog.Debug("Failed to remove address", "error", err)
	}

	n.Cluster.ShutdownStorage()

	n.cancel()

	return nil
}

// Start the node and begin monitoring its state and heartbeat.
func (n *Node) Start() chan bool {
	n.startedAt = time.Now()
	n.replica = NewNodeReplica(n)

	n.heartbeat()
	n.Tick()
	go n.monitorPrimary()
	go n.runTicker()

	defer func() {
		n.started <- true
	}()

	return n.started
}

// If the node is the primary, step down from the primary role.
func (n *Node) StepDown() error {
	if !n.IsPrimary() {
		return nil
	}

	if err := n.Lease().Release(); err != nil {
		return err
	}

	n.Primary().Shutdown()

	n.SetMembership(ClusterMembershipReplica)

	return nil
}

// Store the address of the node in the cluster's network file system.
func (n *Node) StoreAddress() error {
tryStore:
	timeBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(timeBytes, uint64(time.Now().Unix()))
	err := n.Cluster.NetworkFS().WriteFile(n.AddressPath(), timeBytes, 0644)

	if err != nil {
		if !os.IsNotExist(err) {
			return err
		}

		err = n.Cluster.NetworkFS().MkdirAll(n.Cluster.NodePath(), 0755)

		if err != nil {
			return err
		}

		goto tryStore
	}

	n.storedAddressAt = time.Now()

	return nil
}

// Tick the node to perform the necessary checks and operations for cluster
// membership and state.
func (n *Node) Tick() {
	if n.joinedClusterAt.IsZero() {
		n.JoinCluster()
	}

	n.mutex.Lock()
	n.LastActive = time.Now()
	n.mutex.Unlock()

	if n.State == NodeStateIdle {
		n.State = NodeStateActive
	}
}

func (n *Node) Timestamp() time.Time {
	return n.startedAt
}

// truncateFile truncates the specified file. It creates the file if it does not exist.
func (n *Node) truncateFile(filePath string) error {
	return n.Cluster.NetworkFS().WriteFile(filePath, []byte(""), os.ModePerm)
}

func (n *Node) WALSynchronizer() NodeWalSynchronizer {
	return n.walSynchronizer
}
