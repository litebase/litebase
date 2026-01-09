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

	"github.com/litebase/litebase/internal/utils"
	"github.com/litebase/litebase/pkg/cluster/messages"
	"github.com/litebase/litebase/pkg/storage"
)

const (
	NodeHeartbeatInterval = 1 * time.Second
	NodeIdleTimeout       = 60 * time.Second
	NodeStateActive       = "active"
	NodeStateIdle         = "idle"
)

var (
	NodeStoreAddressInterval = 5 * time.Second
	NodeTickTimeout          = 3 * time.Second
)

var addressProvider func() string
var addressProviderMutex sync.Mutex
var privatePortProvider func() int
var privatePortProviderMutex sync.Mutex
var publicPortProvider func() int
var publicPortProviderMutex sync.Mutex

type Node struct {
	address            string
	cancel             context.CancelFunc
	Cluster            *Cluster
	context            context.Context
	electionMoratorium time.Time
	Election           *ClusterElection
	Elections          []*ClusterElection
	Initialized        bool
	joinedClusterAt    time.Time
	lastTick           time.Time
	lease              *Lease
	LastActive         time.Time
	lastActiveMutex    *sync.Mutex
	ID                 string
	membership         string
	mutex              *sync.Mutex
	onStarted          func()
	pageLoggerAccessor NodePageLoggerAccessor
	primaryAddress     string
	primary            *NodePrimary
	PrimaryHeartbeat   time.Time
	queryBuilder       NodeQueryBuilder
	queryResponsePool  NodeQueryResponsePool
	replica            *NodeReplica
	requestTicker      *time.Ticker
	started            chan bool
	State              string
	startedAt          time.Time
	storedAddressAt    time.Time
	walSynchronizer    NodeWalSynchronizer
}

// Create a new instance of a node.
func NewNode(cluster *Cluster) *Node {
	node := &Node{
		address:         "",
		Cluster:         cluster,
		LastActive:      time.Time{},
		lastActiveMutex: &sync.Mutex{},
		membership:      ClusterMembershipReplica,
		mutex:           &sync.Mutex{},
		started:         make(chan bool, 1),
		State:           NodeStateActive,
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

	n.mutex.Lock()
	defer n.mutex.Unlock()

	addressProviderMutex.Lock()
	defer addressProviderMutex.Unlock()

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

	n.address = fmt.Sprintf("%s:%s", address, n.getPort())

	return n.address, nil

}

// Get the port to use for the node address (private port if available, otherwise public port)
func (n *Node) getPort() string {
	// First check if we have a fixed private port configured (non-zero)
	if n.Cluster.Config.PrivatePort != "" && n.Cluster.Config.PrivatePort != "0" {
		return n.Cluster.Config.PrivatePort
	}

	// If private port is 0 (auto-assign), check if we have a provider for the actual assigned port
	privatePortProviderMutex.Lock()
	defer privatePortProviderMutex.Unlock()

	if privatePortProvider != nil {
		privatePort := privatePortProvider()

		if privatePort > 0 {
			return fmt.Sprintf("%d", privatePort)
		}
	}

	return n.Cluster.Config.PrivatePort

}

// Get the public port to use for public API requests
func (n *Node) getPublicPort() string {
	// First check if we have a fixed public port configured
	if n.Cluster.Config.Port != "" && n.Cluster.Config.Port != "0" {
		return n.Cluster.Config.Port
	}

	// If public port is auto-assigned, check if we have a provider for the actual assigned port
	publicPortProviderMutex.Lock()
	defer publicPortProviderMutex.Unlock()

	if publicPortProvider != nil {
		publicPort := publicPortProvider()

		if publicPort > 0 {
			return fmt.Sprintf("%d", publicPort)
		}
	}

	// Fallback to the configured port
	return n.Cluster.Config.Port
}

func (n *Node) PageLoggerAccessor() NodePageLoggerAccessor {
	return n.pageLoggerAccessor
}

// Get the public address for API requests (as opposed to the private address used for cluster communication)
func (n *Node) PublicAddress() (string, error) {
	var address string

	addressProviderMutex.Lock()
	defer addressProviderMutex.Unlock()

	if addressProvider != nil {
		address = addressProvider()
	} else {
		address = "127.0.0.1"
	}

	publicAddress := fmt.Sprintf("%s:%s", address, n.getPublicPort())

	return publicAddress, nil
}

// Return the path for where the address will be stored.
func (n *Node) AddressPath() string {
	// Replace the colon in the address with an underscore
	address, _ := n.Address()

	address = strings.ReplaceAll(address, ":", "_")
	return fmt.Sprintf("%s%s", n.Cluster.NodePath(), address)
}

// Add a peer election to the node.
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
	if n.storedAddressAt.IsZero() || time.Since(n.storedAddressAt) > 5*time.Second {
		// Check if the address is already stored
		if _, err := n.Cluster.NetworkFS().Stat(n.AddressPath()); err == nil {
			return n.StoreAddress()
		}

		// If the address is not stored, the node needs to rejoin the cluster
		n.joinedClusterAt = time.Time{}

		err := n.JoinCluster()

		if err != nil {
			return err
		}
	}

	return nil
}

func (n *Node) GetMembership() string {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	return n.membership
}

// Check if a peer node has an election running.
func (n *Node) HasPeerElectionRunning() bool {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	n.purgeExpiredElections()

	return len(n.Elections) > 0
}

// Trigger the node to perform a heartbeat.
func (n *Node) heartbeat() {
	n.mutex.Lock()

	err := n.ensureNodeAddressStored()

	if err != nil {
		slog.Debug("Failed to ensure node address is stored", "error", err)
	}

	if n.membership == ClusterMembershipPrimary {
		n.mutex.Unlock()

		lease := n.Lease()

		if lease == nil {
			err := n.removePrimaryStatus()

			if err != nil {
				slog.Debug("Failed to remove primary status", "error", err)
			}

			n.setMembership(ClusterMembershipReplica)

			return
		}

		if lease.ShouldRenew() {
			err := lease.Renew()

			if err != nil {
				slog.Debug("Failed to renew lease", "error", err)

				err := n.removePrimaryStatus()

				if err != nil {
					slog.Debug("Failed to remove primary status", "error", err)
				}
			}
		} else if lease.IsExpired() {
			// Check if lease has expired (e.g., after a pause)
			slog.Debug("Lease has expired, stepping down")

			err := n.StepDown()

			if err != nil {
				slog.Debug("Failed to remove primary status after lease expiration", "error", err)
			}
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

	select {
	case <-n.context.Done():
		return
	default:
		break
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
	if _, err := n.Cluster.NetworkFS().Stat(n.Cluster.NodePath()); os.IsNotExist(err) {
		err := n.Cluster.NetworkFS().Mkdir(n.Cluster.NodePath(), 0750)

		if err != nil {
			slog.Error("Failed to create node directory", "error", err)
			return
		}
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
	n.mutex.Lock()

	// If an election is running, wait for it to finish
	if n.Election != nil && n.Election.Running() {
		select {
		case <-n.Election.Context().Done():
		default:
			break
		}
	}

	// Check if we need to tick, but don't call Tick() while holding mutex
	var needsTick bool
	n.lastActiveMutex.Lock()
	needsTick = n.LastActive.IsZero() || time.Since(n.LastActive) > 5*time.Minute
	n.lastActiveMutex.Unlock()

	// Release mutex before calling Tick()
	if needsTick {
		n.mutex.Unlock()
		n.Tick()
		n.mutex.Lock() // Re-acquire mutex
	}

	if n.membership == ClusterMembershipReplica {
		n.mutex.Unlock()
		return false
	}

	// If the cluster membership is primary and the lease is still valid
	if n.membership == ClusterMembershipPrimary &&
		n.lease != nil &&
		n.lease.IsUpToDate() {
		n.mutex.Unlock()
		return true
	}

	isPrimary := n.primaryFileVerification()
	n.mutex.Unlock()
	return isPrimary
}

func (n *Node) IsReplica() bool {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	// If an election is running, wait for it to finish
	if n.Election != nil && n.Election.Running() {
		select {
		case <-n.Election.Context().Done():
		default:
			break
		}
	}

	return n.membership == ClusterMembershipReplica && n.replica != nil
}

func (n *Node) JoinCluster() error {
	if !n.joinedClusterAt.IsZero() {
		return nil
	}

	if err := n.StoreAddress(); err != nil {
		return err
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
			n.joinedClusterAt = time.Now().UTC()
		}
	} else {
		n.joinedClusterAt = time.Now().UTC()
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
	n.mutex.Lock()
	defer n.mutex.Unlock()

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

// On started hook.
func (n *Node) OnStarted(callback func()) {
	n.onStarted = callback
}

// Return the peer elections that the node is aware of.
func (n *Node) PeerElections() []*ClusterElection {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	n.purgeExpiredElections()

	return n.Elections
}

// Return the primary node of the cluster.
func (n *Node) Primary() *NodePrimary {
	return n.primary
}

// Return the primary address of the node. If the primary address is not set,
// it will read the primary file from the cluster's network file system.
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

// Return the primary public address for API requests
func (n *Node) PrimaryPublicAddress() string {
	primaryPrivateAddress := n.PrimaryAddress()
	if primaryPrivateAddress == "" {
		return ""
	}

	// If this node is the primary, return its own public address
	if n.IsPrimary() {
		if publicAddr, err := n.PublicAddress(); err == nil {
			return publicAddr
		}
	}

	// For replica nodes, we need to derive the primary's public address
	// This assumes the primary has the same host but different port
	// Parse the private address to get the host
	if colonIndex := strings.LastIndex(primaryPrivateAddress, ":"); colonIndex != -1 {
		primaryHost := primaryPrivateAddress[:colonIndex]

		// Use the public port provider to get the primary's public port
		publicPortProviderMutex.Lock()
		defer publicPortProviderMutex.Unlock()

		if publicPortProvider != nil {
			// For now, we assume the public port is configured in the config
			// In a more sophisticated setup, we might need to store both addresses
			return fmt.Sprintf("%s:%s", primaryHost, n.Cluster.Config.Port)
		}
	}

	// Fallback: assume private and public addresses are the same (single server setup)
	return primaryPrivateAddress
}

func (n *Node) primaryLeaseVerification() bool {
	n.mutex.Lock()
	primaryHeartBeatIsZero := n.PrimaryHeartbeat.IsZero()
	timeSincePrimaryHeartbeat := time.Since(n.PrimaryHeartbeat)
	n.mutex.Unlock()

	if n.IsReplica() && !primaryHeartBeatIsZero && timeSincePrimaryHeartbeat < 3*time.Second {
		return true
	}

	primaryData, err := n.Cluster.NetworkFS().ReadFile(n.Cluster.PrimaryPath())

	if err != nil {
		slog.Debug("Failed to read primary file", "error", err, "address", n.address)
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

	if time.Now().UTC().Unix() >= leaseTime {
		err := n.removePrimaryStatus()

		if err != nil {
			slog.Debug("Failed to remove primary status", "error", err)
		}

		n.setMembership(ClusterMembershipReplica)

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

	if time.Now().UTC().Unix() < leaseTime {
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
		slog.Debug("Failed to release lease", "error", err)

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
	if time.Now().UTC().Before(n.electionMoratorium) {
		return false, ErrElectionMoratorium
	}

	n.mutex.Lock()
	if n.Election != nil && n.Election.Running() {
		n.mutex.Unlock()

		return false, ErrElectionAlreadyRunning
	}

	if n.Election == nil || n.Election.Stopped() {
		n.Election = NewClusterElection(n)
	}

	election := n.Election
	n.mutex.Unlock()

	defer func() {
		if election.Stopped() {
			return
		}

		election.Stop()
	}()

	elected, err := election.run()

	if err != nil {
		return false, err
	}

	if !elected {
		return elected, nil
	}

	n.SetMembership(ClusterMembershipPrimary)

	n.mutex.Lock()
	n.lease = NewLease(n)
	lease := n.lease
	n.mutex.Unlock()

	err = lease.Renew()

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
			// Check if the ticker is resuming after a pause
			if !n.lastTick.IsZero() && time.Now().UTC().After(n.lastTick.Add(NodeTickTimeout)) {
				if n.IsPrimary() {
					err := n.StepDown()

					if err != nil {
						slog.Error("Error stepping down", "error", err)
					}
				}
			}

			n.lastTick = time.Now().UTC()

			// Continue if the node is idle
			if n.State == NodeStateIdle {
				continue
			}

			n.lastActiveMutex.Lock()
			lastActive := n.LastActive

			// Continue if the node has not been inactive for the idle timeout duration
			if lastActive.IsZero() || time.Since(lastActive) <= NodeIdleTimeout {
				n.lastActiveMutex.Unlock()
				continue
			}

			n.lastActiveMutex.Unlock()

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

	url := fmt.Sprintf("http://%s/v1/events", node.Address)

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

	defer func() {
		if err := res.Body.Close(); err != nil {
			slog.Error("Error closing response body", "error", err)
		}
	}()

	if res.StatusCode >= 400 {
		return fmt.Errorf("failed to send message: %d", res.StatusCode)
	}

	return nil
}

func SetAddressProvider(provider func() string) {
	addressProviderMutex.Lock()
	defer addressProviderMutex.Unlock()

	addressProvider = provider
}

func SetPrivatePortProvider(provider func() int) {
	privatePortProviderMutex.Lock()
	defer privatePortProviderMutex.Unlock()

	privatePortProvider = provider
}

func SetPublicPortProvider(provider func() int) {
	publicPortProviderMutex.Lock()
	defer publicPortProviderMutex.Unlock()

	publicPortProvider = provider
}

// ResetPortProviders clears all port providers (useful for testing)
func ResetPortProviders() {
	privatePortProviderMutex.Lock()
	publicPortProviderMutex.Lock()
	defer privatePortProviderMutex.Unlock()
	defer publicPortProviderMutex.Unlock()

	privatePortProvider = nil
	publicPortProvider = nil
}

func (n *Node) setInternalHeaders(req *http.Request) error {
	address, _ := n.Address()

	encryptedHeader, err := n.Cluster.Auth.SecretsManager.Encrypt(
		n.Cluster.Config.EncryptionKey,
		[]byte(address),
	)

	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Litebase-Node", string(encryptedHeader))
	req.Header.Set("X-Litebase-Node-Timestamp", fmt.Sprintf("%d", time.Now().UTC().UnixNano()))

	return nil
}

// Set the membership of the node in the cluster.
func (n *Node) SetMembership(membership string) {
	n.mutex.Lock()
	prevMembership := n.membership
	n.membership = membership
	n.mutex.Unlock()

	// Perform side effects without holding the lock
	if membership == ClusterMembershipPrimary {
		primary := NewNodePrimary(n)

		n.mutex.Lock()
		n.primary = primary
		replica := n.replica
		n.replica = nil
		n.mutex.Unlock()

		if replica != nil {
			err := replica.Stop()

			if err != nil {
				slog.Debug("Failed to stop replica", "error", err)
			}
		}

		// Ensure the primary checks for dirty files that need to be synced from
		// tiered storage.
		if driver, ok := n.Cluster.TieredFS().Driver().(*storage.TieredFileSystemDriver); ok {
			err := driver.SyncDirtyFiles()

			if err != nil {
				slog.Debug("Failed to sync dirty files", "error", err)
			}
		}
	}

	if membership == ClusterMembershipReplica && prevMembership != ClusterMembershipPrimary && n.PrimaryAddress() != "" {
		replica := NewNodeReplica(n)
		n.mutex.Lock()
		n.replica = replica
		n.mutex.Unlock()
	}
}

// Set the membership of the node in the cluster (internal, assumes caller handles locking).
func (n *Node) setMembership(membership string) {
	n.membership = membership
}

// Set the page logger accessor for the node.
func (n *Node) SetPageLoggerAccessor(pageLoggerAccessor NodePageLoggerAccessor) {
	n.pageLoggerAccessor = pageLoggerAccessor
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
			err := n.Lease().Release()

			if err != nil {
				slog.Debug("Failed to release lease", "error", err)
			}
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
	n.mutex.Lock()
	n.startedAt = time.Now().UTC()
	n.replica = NewNodeReplica(n)
	n.mutex.Unlock()

	n.heartbeat()
	n.Tick()
	go n.monitorPrimary()
	go n.runTicker()

	defer func() {
		// Run onStarted callback BEFORE signaling completion
		// This ensures migrations and other initialization complete before tests proceed
		if n.onStarted != nil {
			n.onStarted()
		}

		n.started <- true
		close(n.started)
	}()

	return n.started
}

// If the node is the primary, step down from the primary role.
func (n *Node) StepDown() error {
	if !n.IsPrimary() {
		n.lease = nil

		return nil
	}

	err := n.removePrimaryStatus()

	if err != nil {
		slog.Error("Error removing primary status", "error", err)
	}

	if n.Primary() != nil {
		n.Primary().Shutdown()
	}

	n.setMembership(ClusterMembershipReplica)

	n.primaryAddress = ""

	n.electionMoratorium = time.Now().UTC().Add(1 * time.Second)

	return nil
}

// Store the address of the node in the cluster's network file system.
func (n *Node) StoreAddress() error {
tryStore:
	timeBytes := make([]byte, 8)

	uint64Time, err := utils.SafeInt64ToUint64(time.Now().UTC().Unix())

	if err != nil {
		return err
	}

	binary.LittleEndian.PutUint64(timeBytes, uint64Time)

	err = n.Cluster.NetworkFS().WriteFile(n.AddressPath(), timeBytes, 0600)

	if err != nil {
		if !os.IsNotExist(err) {
			return err
		}

		err = n.Cluster.NetworkFS().MkdirAll(n.Cluster.NodePath(), 0750)

		if err != nil {
			return err
		}

		goto tryStore
	}

	n.storedAddressAt = time.Now().UTC()

	return nil
}

// Tick the node to perform the necessary checks and operations for cluster
// membership and state.
func (n *Node) Tick() {
	if n.joinedClusterAt.IsZero() {
		err := n.JoinCluster()

		if err != nil {
			slog.Error("Failed to join cluster", "error", err)
		}
	}

	n.lastActiveMutex.Lock()
	n.LastActive = time.Now().UTC()
	n.lastActiveMutex.Unlock()

	if n.State == NodeStateIdle {
		n.State = NodeStateActive
	}
}

// Return the started at timestamp of the node.
func (n *Node) Timestamp() time.Time {
	return n.startedAt
}

// Wait for the current node to become the primary or a new primary to be elected.
func (n *Node) WaitForPrimary() error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	timeout := time.After(30 * time.Second)

	for {
		select {
		case <-timeout:
			return fmt.Errorf("timeout waiting for primary election")
		case <-n.context.Done():
			return n.context.Err()
		case <-ticker.C:
			// Check if this node is primary
			if n.IsPrimary() {
				return nil
			}

			// Check if another node is primary
			if n.PrimaryAddress() != "" && n.primaryLeaseVerification() {
				return nil
			}
		}
	}
}

func (n *Node) WALSynchronizer() NodeWalSynchronizer {
	return n.walSynchronizer
}
