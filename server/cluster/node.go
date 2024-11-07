package cluster

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"litebase/server/cluster/messages"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	NodeHeartbeatInterval = 1 * time.Second
	NodeHeartbeatTimeout  = 1 * time.Second
	NodeIdleTimeout       = 60 * time.Second
	NodeStateActive       = "active"
	NodeStateIdle         = "idle"
)

var addressProvider func() string

type Node struct {
	address                 string
	cancel                  context.CancelFunc
	cluster                 *Cluster
	context                 context.Context
	electionMoratorium      time.Time
	election                *ClusterElection
	electionRunning         bool
	hasNomination           bool
	initialized             bool
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
	ReplicationGroupManager *NodeReplicationGroupManager
	requestTicker           *time.Ticker
	State                   string
	standBy                 chan struct{}
	startedAt               time.Time
	storedAddressAt         time.Time
	walReplicator           *NodeWalReplicator
	walSynchronizer         NodeWalSynchronizer
}

// Create a new instance of a node.
func NewNode(cluster *Cluster) *Node {
	node := &Node{
		address:    "",
		cluster:    cluster,
		lastActive: time.Time{},
		Membership: ClusterMembershipStandBy,
		mutex:      &sync.Mutex{},
		standBy:    make(chan struct{}),
		State:      NodeStateActive,
	}

	node.ReplicationGroupManager = NewNodeReplicationGroupManager(node)

	hash := sha256.Sum256([]byte(node.Address()))
	node.Id = hex.EncodeToString(hash[:])
	node.context, node.cancel = context.WithCancel(context.Background())

	return node
}

// Get the address of the node.
func (n *Node) Address() string {
	if n.address != "" {
		return n.address
	}

	var address string
	var err error

	if addressProvider != nil {
		address = addressProvider()
	} else {
		address, err = os.Hostname()

		if err != nil {
			log.Fatal(err)
		}
	}

	n.address = fmt.Sprintf("%s:%s", address, n.cluster.Config.Port)

	return n.address

}

// Return the path for where the address will be stored.
func (n *Node) AddressPath() string {
	// Replace the colon in the address with an underscore
	address := strings.ReplaceAll(n.Address(), ":", "_")

	return fmt.Sprintf("%s%s", n.cluster.NodePath(), address)
}

// Return the context for the node.
func (n *Node) Context() context.Context {
	return n.context
}

func (n *Node) Election() *ClusterElection {
	if n.election == nil {
		n.election = NewClusterElection(n, time.Now())
	}

	return n.election
}

// Trigger the node to perform a heartbeat.
func (n *Node) Heartbeat() {
	if n.Membership == ClusterMembershipPrimary {
		if LeaseDuration-time.Since(n.LeaseRenewedAt) < 10*time.Second {
			n.renewLease()
		} else {
			err := n.Primary().Heartbeat()

			if err != nil {
				log.Println(err)
			}
		}

		return
	}

	if n.context.Err() != nil {
		return
	}

	if !n.IsStandBy() && !n.primaryLeaseVerification() {
		success := n.RunElection()

		if !success && n.electionMoratorium.IsZero() {
			n.SetElectionMoratorium()
		}
	}
}

// Initialize the node with the query builder and wal synchronizer.
func (n *Node) Init(queryBuilder NodeQueryBuilder, walSynchronizer NodeWalSynchronizer) {
	registerNodeMessages()

	// Make directory if it doesn't exist
	if n.cluster.ObjectFS().Stat(n.cluster.NodePath()); os.IsNotExist(os.ErrNotExist) {
		n.cluster.ObjectFS().Mkdir(n.cluster.NodePath(), 0755)
	}

	n.SetQueryBuilder(queryBuilder)
	n.SetWalSchronizer(walSynchronizer)

	n.initialized = true
}

func (n *Node) IsIdle() bool {
	return n.State == NodeStateIdle
}

func (n *Node) IsPrimary() bool {
	// If the node has not been activatedf, tick it before running these checks
	if n.lastActive.IsZero() {
		n.Tick()
	}

	if n.Membership == ClusterMembershipReplica || n.Membership == ClusterMembershipStandBy {
		return false
	}

	// If the cluster membership is primary and the lease is still valid
	if n.Membership == ClusterMembershipPrimary && time.Now().Unix() < n.LeaseExpiresAt {
		return true
	}

	return n.primaryFileVerification()
}

func (n *Node) IsReplica() bool {
	return n.Membership == ClusterMembershipReplica && n.replica != nil
}

func (n *Node) IsStandBy() bool {
	return n.Membership == ClusterMembershipStandBy
}

func (n *Node) joinCluster() error {
	if !n.joinedClusterAt.IsZero() {
		return nil
	}

	if n.storedAddressAt.IsZero() {
		if err := n.storeAddress(); err != nil {
			return err
		}
	}

	// The Node should be added to the cluster map
	err := n.cluster.AddMember(n.cluster.Config.NodeType, n.Address())

	if err != nil {
		log.Println(err)
		return err
	}

	// Check if the node has joined the cluster
	if n.PrimaryAddress() != "" && n.PrimaryAddress() != n.Address() && n.replica != nil && n.joinedClusterAt.IsZero() {
		err := n.replica.JoinCluster()

		if err != nil {
			log.Println(err)
		} else {
			n.joinedClusterAt = time.Now()
		}
	} else {
		n.joinedClusterAt = time.Now()
	}

	err = n.cluster.Broadcast("cluster:join", map[string]string{
		"address": n.Address(),
		"group":   n.cluster.Config.NodeType,
	})

	if err != nil {
		log.Println(err)
		return err
	}

	return nil
}

func (n *Node) monitorPrimary() {
	n.Heartbeat()

	ticker := time.NewTicker(NodeHeartbeatInterval)
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

func (n *Node) Primary() *NodePrimary {
	return n.primary
}

func (n *Node) PrimaryAddress() string {
	if n.primaryAddress == "" {
		primaryData, err := n.cluster.ObjectFS().ReadFile(n.cluster.PrimaryPath())

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

	primaryData, err := n.cluster.ObjectFS().ReadFile(n.cluster.PrimaryPath())

	if err != nil {
		// log.Printf("Failed to read primary file: %v", err)
		return false
	}

	// There is a primary file but it is empty
	if len(primaryData) == 0 {
		return false
	}

	// Check if the primary is still alive
	leaseData, err := n.cluster.ObjectFS().ReadFile(n.cluster.LeasePath())

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
		n.setMembership(ClusterMembershipReplica)

		return false
	}

	return true
}

func (n *Node) primaryFileVerification() bool {
	// Check if the primary file exists and is not empty
	if primaryData, err := n.cluster.ObjectFS().ReadFile(n.cluster.PrimaryPath()); err != nil || len(primaryData) == 0 || string(primaryData) != n.Address() {
		if err != nil && !os.IsNotExist(err) {
			log.Printf("Error accessing primary file: %v", err)
		}

		return false
	}

	// Check if the lease file exists, is not empty, and has a valid future timestamp
	leaseData, err := n.cluster.ObjectFS().ReadFile(n.cluster.LeasePath())

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

// As the Primary, publish messages to the replicas of the cluster group.
func (n *Node) Publish(message messages.NodeMessage) error {
	if n.primary == nil {
		return errors.New("node is not the primary")
	}

	return n.primary.Publish(message)
}

// Release the lease and remove the primary status from the node. This should
// be called before changing the cluster membership to replica.

func (n *Node) releaseLease() error {
	n.LeaseExpiresAt = 0

	if n.Membership != ClusterMembershipPrimary {
		return fmt.Errorf("node is not a leader")
	}

	if err := n.truncateFile(n.cluster.PrimaryPath()); err != nil {
		return err
	}

	if err := n.truncateFile(n.cluster.LeasePath()); err != nil {
		return err
	}

	return nil
}

func (n *Node) Replica() *NodeReplica {
	return n.replica
}

// Return the NodeWalReplicator for the Node.
func (n *Node) WalReplicator() *NodeWalReplicator {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	if n.walReplicator == nil {
		n.walReplicator = NewNodeReplicator(n)
	}

	return n.walReplicator
}

// Return the query builder of the node.
func (n *Node) QueryBuilder() NodeQueryBuilder {
	return n.queryBuilder
}

func (n *Node) removeAddress() error {
	return n.cluster.ObjectFS().Remove(n.AddressPath())
}

func (n *Node) removePrimaryStatus() error {
	// Release the lease
	n.releaseLease()

	if n.primary != nil {
		n.primary = nil
	}

	return nil
}

func (n *Node) renewLease() error {
	if n.Membership != ClusterMembershipPrimary {
		return fmt.Errorf("node is not a leader")
	}

	if err := n.context.Err(); err != nil {
		log.Println("Operation canceled before starting.")
		return err
	}

	// Verify the primary is stil the current node
	primaryAddress, err := n.cluster.ObjectFS().ReadFile(n.cluster.PrimaryPath())

	if err != nil {
		return err
	}

	if string(primaryAddress) != n.Address() {
		n.setMembership(ClusterMembershipReplica)

		return fmt.Errorf("primary address verification failed")
	}

	if err := n.context.Err(); err != nil {
		log.Println("Operation canceled before starting.")
		return err
	}

	expiresAt := time.Now().Add(LeaseDuration).Unix()
	leaseTimestamp := strconv.FormatInt(expiresAt, 10)

	err = n.cluster.ObjectFS().WriteFile(n.cluster.LeasePath(), []byte(leaseTimestamp), os.ModePerm)

	if err != nil {
		log.Printf("Failed to write lease file: %v", err)
		return err
	}

	if err := n.context.Err(); err != nil {
		log.Println("Operation canceled before starting.")
		return err
	}

	// Verify the Lease file has the written value
	leaseData, err := n.cluster.ObjectFS().ReadFile(n.cluster.LeasePath())

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

// Run an election to determine the primary node in the cluster group.
func (n *Node) RunElection() bool {
	if n.electionRunning {
		return false
	}

	if !n.electionMoratorium.IsZero() && time.Now().Before(n.electionMoratorium) {
		return false
	}

	defer func() {
		n.electionRunning = false

		if n.election == nil {
			return
		}

		n.mutex.Lock()
		n.election.Stop()
		n.election = nil
		n.mutex.Unlock()
	}()

	n.electionRunning = true

	n.mutex.Lock()

	if n.election == nil {
		n.election = NewClusterElection(n, time.Now())
	}

	n.mutex.Unlock()

	for i := 0; i < 3; i++ {
		nominated, err := n.election.Run()

		if err != nil {
			return false
		}

		if !nominated {
			return false
		}
	}

	// Write the current address to the nomination file
	success, err := n.writeNomination()

	if err != nil {
		return false
	}

	if !success {
		return false
	}

	n.hasNomination = true

	// Confirm the election
	confirmed := n.runElectionConfirmation()

	if !confirmed {
		return false
	}

	// Verify that the nomination file is still valid
	verified, err := n.verifyNomination()

	if err != nil {
		return false
	}

	if !verified {
		return false
	}

	// Confirm the election
	confirmed = n.runElectionConfirmation()

	if !confirmed {
		return false
	}

	// Verify that the nomination file is still valid
	verified, err = n.verifyNomination()

	if err != nil {
		return false
	}

	if !verified {
		return false
	}

	err = n.cluster.ObjectFS().WriteFile(
		n.cluster.PrimaryPath(),
		[]byte(n.Address()),
		0644,
	)

	if err != nil {
		log.Printf("Failed to write primary file: %v", err)
		return false
	}

	n.setMembership(ClusterMembershipPrimary)
	n.truncateFile(n.cluster.NominationPath())

	err = n.renewLease()

	return err == nil

}

func (n *Node) runElectionConfirmation() bool {
	nodeIdentifiers := n.cluster.NodeGroupVotingNodes()

	// If there is only one node in the group, it is the current node and the
	// election is confirmed.
	if len(nodeIdentifiers) <= 1 {
		return true
	}

	data := map[string]interface{}{
		"address":   n.Address(),
		"group":     n.cluster.Config.NodeType,
		"timestamp": n.election.StartedAt.UnixNano(),
	}

	jsonData, err := json.Marshal(data)

	if err != nil {
		log.Println("Failed to marshal election data: ", err)
		return false
	}

	votes := make(chan bool, len(nodeIdentifiers)-1)

	for _, nodeIdentifier := range nodeIdentifiers {
		if nodeIdentifier.String() == n.Address() {
			continue
		}

		go func(nodeIdentifier *NodeIdentifier) {
			request, err := http.NewRequestWithContext(
				n.context,
				"POST",
				fmt.Sprintf("http://%s/cluster/election/confirmation", nodeIdentifier.String()),
				bytes.NewBuffer(jsonData),
			)

			if err != nil {
				log.Println("Failed to create confirmation election request: ", err)
				votes <- false
				return
			}

			request.Header.Set("Content-Type", "application/json")

			err = n.setInternalHeaders(request)

			if err != nil {
				log.Println("Failed to set internal headers: ", err)
				votes <- false
				return
			}

			resp, err := http.DefaultClient.Do(request)

			if err != nil {
				log.Printf(
					"Error sending election confirmation request to node %s from node %s: %s",
					nodeIdentifier.String(),
					n.Address(),
					err,
				)
			}

			defer resp.Body.Close()

			if resp.StatusCode == http.StatusOK {
				votes <- true
			} else {
				votes <- false
			}
		}(nodeIdentifier)
	}

	// Wait for a response from each node in the group
	votesReceived := 1
	votesNeeded := len(nodeIdentifiers)/2 + 1
	timeout := time.After(3 * time.Second) // Set a timeout duration

	for i := 0; i < len(nodeIdentifiers)-1; i++ {
		select {
		case <-timeout:
			return false
		case <-n.context.Done():
			return false
		case vote := <-votes:

			if vote {
				votesReceived++
			}

			if votesReceived >= votesNeeded {
				return true
			}
		}
	}

	return false
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

			// Continue if the node has not been inactive for the idle timeout duration
			if n.lastActive.IsZero() || time.Since(n.lastActive) <= NodeIdleTimeout {
				continue
			}

			// Change the cluster membership to stand by
			// n.setMembership(ClusterMembershipStandBy)
		}
	}
}

func (n *Node) setMembership(membership string) {
	prevMembership := n.Membership

	n.Membership = membership
	// Forget the last known primary address
	n.primaryAddress = ""

	if membership == ClusterMembershipPrimary {
		n.primary = NewNodePrimary(n)

		if n.replica != nil {
			n.replica.Stop()
			n.replica = nil
		}
	}

	if membership == ClusterMembershipReplica && prevMembership != ClusterMembershipPrimary && n.PrimaryAddress() != "" {
		n.replica = NewNodeReplica(n)

		if n.primary != nil {
			n.removePrimaryStatus()
		}
	}

	if membership == ClusterMembershipStandBy {
		n.State = NodeStateIdle
	}
}

func (n *Node) SetQueryBuilder(queryBuilder NodeQueryBuilder) {
	n.queryBuilder = queryBuilder
}

func (n *Node) SetWalSchronizer(synchronizer NodeWalSynchronizer) {
	n.walSynchronizer = synchronizer
}

func (n *Node) Shutdown() error {
	n.mutex.Lock()
	defer n.mutex.Unlock()

	n.cluster.ShutdownStorage()

	if n.IsPrimary() {
		n.Primary().Shutdown()
		n.removePrimaryStatus()
	}

	err := n.cluster.Broadcast("cluster:leave", map[string]string{
		"address": n.Address(),
		"group":   n.cluster.Config.NodeType,
	})

	if err != nil {
		log.Println(err)
	}

	if n.IsReplica() {
		n.replica.LeaveCluster()
	}

	err = n.removeAddress()

	if err != nil && !os.IsNotExist(err) {
		log.Println(err)
	}

	n.cancel()

	return nil
}

func (n *Node) Start() error {
	n.startedAt = time.Now()

	go n.monitorPrimary()
	go n.runTicker()
	n.Tick()

	return nil
}

func (n *Node) storeAddress() error {
tryStore:
	err := n.cluster.ObjectFS().WriteFile(n.AddressPath(), []byte(n.Address()), 0644)

	if err != nil {
		if !os.IsNotExist(err) {
			return err
		}

		err = n.cluster.ObjectFS().MkdirAll(n.cluster.NodePath(), 0755)

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
	// Check if the is still registered as primary
	if n.lastActive.IsZero() {
		if n.primaryFileVerification() {
			n.setMembership(ClusterMembershipPrimary)
		}
	}

	if n.joinedClusterAt.IsZero() {
		n.joinCluster()
	}

	n.lastActive = time.Now()

	if n.State == NodeStateIdle {
		n.State = NodeStateActive
	}

	// If the node is a standby, and it hasn't won the election at this point,
	// it should manually become a replica and ensure it has membership.
	if n.Membership == ClusterMembershipStandBy {
		n.setMembership(ClusterMembershipReplica)

		n.Heartbeat()
	}
}

func (n *Node) Send(message interface{}) (interface{}, error) {
	return n.replica.Send(message)
}

func (n *Node) SendEvent(node *NodeIdentifier, message NodeEvent) error {
	// Check if the context is canceled
	if n.context.Err() != nil {
		return fmt.Errorf("node context is canceled")
	}

	url := fmt.Sprintf("http://%s:%s/events", node.Address, node.Port)

	data, err := json.Marshal(message)

	if err != nil {
		log.Println(err)
		return err
	}

	req, err := http.NewRequestWithContext(n.context, "POST", url, bytes.NewBuffer(data))

	if err != nil {
		log.Println(err)
		return err
	}

	err = n.setInternalHeaders(req)

	if err != nil {
		log.Println(err)
		return err
	}

	client := &http.Client{
		Timeout: 1 * time.Second,
	}

	res, err := client.Do(req)

	if err != nil {
		return err
	}

	defer res.Body.Close()

	if res.StatusCode >= 400 {
		return fmt.Errorf("failed to send message")
	}

	return nil
}

func SetAddressProvider(provider func() string) {
	addressProvider = provider
}

func (n *Node) SetElectionMoratorium() {
	n.electionMoratorium = time.Now().Add(ElectionRetryWait)
}

func (n *Node) setInternalHeaders(req *http.Request) error {
	encryptedHeader, err := n.cluster.Auth.SecretsManager.Encrypt(
		n.cluster.Config.Signature,
		n.Address(),
	)

	if err != nil {
		log.Println(err)
		return err
	}

	req.Header.Set("X-Lbdb-Node", encryptedHeader)
	req.Header.Set("X-Lbdb-Node-Timestamp", fmt.Sprintf("%d", n.startedAt.UnixNano()))

	return nil
}

func (n *Node) Timestamp() time.Time {
	return n.startedAt
}

// truncateFile truncates the specified file. It creates the file if it does not exist.
func (n *Node) truncateFile(filePath string) error {
	return n.cluster.ObjectFS().WriteFile(filePath, []byte(""), os.ModePerm)
}

func (n *Node) VerifyElectionConfirmation(address string) (bool, error) {
	if n.context.Err() != nil {
		return false, fmt.Errorf("operation canceled")
	}

	nominationFile, err := n.cluster.ObjectFS().OpenFile(n.cluster.NominationPath(), os.O_RDONLY, 0644)

	if err != nil {
		log.Printf("Failed to open nomination file: %v", err)
		return false, err
	}

	defer nominationFile.Close()

	nominationData, err := io.ReadAll(nominationFile)

	if err != nil {
		return false, err
	}

	scanner := bufio.NewScanner(bytes.NewReader(nominationData))

	// Check if the node has already been nominated
	if scanner.Scan() {
		firstLine := scanner.Text()
		// log.Println("FIRST LINE", firstLine)
		if !strings.HasPrefix(firstLine, address) {
			return false, nil
		}
	}

	return true, nil
}

// Read the nomination file and check if the node has already been nominated. This
// means that the node is at the top of the nomination list and the timestamp
// is within the last second.
func (n *Node) verifyNomination() (bool, error) {
	if n.context.Err() != nil {
		return false, fmt.Errorf("operation canceled")
	}

	if !n.hasNomination {
		return false, nil
	}

	// Reopen the file to read the contents
	nominationFile, err := n.cluster.ObjectFS().OpenFile(n.cluster.NominationPath(), os.O_RDONLY, 0644)

	if err != nil {
		log.Printf("Failed to reopen nomination file: %v", err)
		return false, err
	}

	defer nominationFile.Close()

	nominationData, err := io.ReadAll(nominationFile)

	if err != nil {
		return false, err
	}

	scanner := bufio.NewScanner(bytes.NewReader(nominationData))

	// Check if the node has already been nominated
	if scanner.Scan() {
		firstLine := scanner.Text()

		if strings.HasPrefix(firstLine, n.Address()) {
			timestamp := strings.Split(firstLine, " ")[1]

			timestampInt, err := strconv.ParseInt(timestamp, 10, 64)

			if err != nil {
				return false, err
			}

			// Parse the timestamp
			if time.Now().UnixNano()-timestampInt < time.Second.Nanoseconds() {
				return true, nil
			}
		}
	}

	return false, nil
}

func (n *Node) VerifyPrimaryStatus() bool {
	return n.primaryFileVerification()
}

func (n *Node) WalSynchronizer() NodeWalSynchronizer {
	return n.walSynchronizer
}

// Write the nodes address to the nomination file in attempt to nominate itself
// as the primary node.
func (n *Node) writeNomination() (bool, error) {
	if n.context.Err() != nil {
		return false, fmt.Errorf("operation canceled")
	}

	// Attempt to open the nomination file with exclusive lock
openNomination:
	nominationFile, err := n.cluster.ObjectFS().OpenFile(n.cluster.NominationPath(), os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644)

	if err != nil {
		if !os.IsNotExist(err) {
			log.Printf("Failed to open nomination file: %v", err)
			return false, err
		}

		// Retry if the file does not exist
		err = n.cluster.ObjectFS().MkdirAll(filepath.Dir(n.cluster.NominationPath()), 0755)

		if err != nil {
			log.Printf("Failed to create nomination directory: %v", err)
			return false, err
		}

		goto openNomination
	}

	nominationData, err := io.ReadAll(nominationFile)

	if err != nil {
		log.Printf("Failed to read nomination file: %v", err)
		return false, err
	}

	address := n.Address()
	timestamp := time.Now().UnixNano()
	entry := fmt.Sprintf("%s %d\n", address, timestamp)

	// Read the nomination file and check if it is empty or does not contain
	// this node's address in addition to the timestamp being past 1 second.
	if len(nominationData) == 0 {
		_, err = nominationFile.WriteString(entry)

		if err != nil {
			log.Printf("Failed to write to nomination file: %v", err)
			return false, err
		}
	} else {
		// File is not empty and does not contain this node's address
		// Implement logic to determine if this node should still become primary based on timestamps or other criteria
		scanner := bufio.NewScanner(bytes.NewReader(nominationData))

		// Check if the node has already been nominated
		if scanner.Scan() {
			firstLine := scanner.Text()

			timestamp := strings.Split(firstLine, " ")[1]

			timestampInt, err := strconv.ParseInt(timestamp, 10, 64)

			if err != nil {
				return false, err
			}

			if time.Now().UnixNano()-timestampInt < time.Second.Nanoseconds() {
				return false, nil
			}
		}

		err := nominationFile.Truncate(0)

		if err != nil {
			log.Printf("Failed to truncate nomination file: %v", err)
			return false, err
		}

		_, err = nominationFile.WriteString(entry)

		if err != nil {
			log.Printf("Failed to write to nomination file: %v", err)
			return false, err
		}
	}

	err = nominationFile.Close()

	if err != nil {
		return false, err
	}

	return true, nil
}
