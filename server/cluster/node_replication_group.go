package cluster

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"litebase/server/cluster/messages"
	"litebase/server/storage"
	"log"
	"sync"
	"time"
)

type NodeReplicationGroupRole string

const (
	NodeReplicationGroupQuromTTL                          = 10 * time.Second
	NodeReplicationGroupWriter   NodeReplicationGroupRole = "writer"
	NodeReplicationGroupObserver NodeReplicationGroupRole = "observer"
)

var (
	ErrNotWriterInReplicationGroup   = errors.New("node is not a writer in the replication group")
	ErrNotObserverInReplicationGroup = errors.New("node is not an observer in the replication group")
)

// TODO: Ensure the Learner is checking replicated writes to see which ones are
// past the deadline and need to be durably stored.

// NodeReplicationGroup represents a group of nodes that are replicating data
// between each other. In the context of this application, the nodes are storage
// nodes that are responsible for reading and writing data. Before writes can be
// durably stored, they must be replicated to a quorum of nodes in the group.
type NodeReplicationGroup struct {
	cluster          *Cluster
	formingQuorum    bool
	Members          []NodeReplicationGroupMember
	mutex            *sync.RWMutex
	nodeConnections  map[string]*NodeConnection
	pendingWrites    map[string]messages.ReplicationGroupWriteMessage
	replicatedWrites map[string]*ReplicationGroupReplicatedWrite
	quorumReached    chan struct{}
	quorumReachedAt  time.Time
	writeGroup       bool
}

// Create a new instance of a ReplicationGroupManager.
func NewNodeReplicationGroup(cluster *Cluster) *NodeReplicationGroup {
	return &NodeReplicationGroup{
		cluster:          cluster,
		mutex:            &sync.RWMutex{},
		pendingWrites:    make(map[string]messages.ReplicationGroupWriteMessage),
		Members:          make([]NodeReplicationGroupMember, 0),
		nodeConnections:  make(map[string]*NodeConnection),
		replicatedWrites: make(map[string]*ReplicationGroupReplicatedWrite),
		quorumReached:    make(chan struct{}, 3),
	}
}

func (rg *NodeReplicationGroup) AknowledgeCommit(message messages.ReplicationGroupWriteCommitMessage) error {
	err := rg.waitForQuorum()

	if err != nil {
		return err
	}

	rg.mutex.Lock()
	defer rg.mutex.Unlock()

	if rg.replicatedWrites[message.Key].Key == "" {
		return errors.New("no write found for key")
	}

	// Ensure the proposer is not the current node
	if message.Proposer == rg.cluster.node.Address() {
		return errors.New("proposer current node cannot be the proposer for commit")
	}

	// Ensure the proposer matches the original proposer
	if message.Proposer != rg.replicatedWrites[message.Key].Proposer {
		return errors.New("proposer does not match")
	}

	// Ensure the SHA256 matches
	if message.SHA256 != rg.replicatedWrites[message.Key].SHA256 {
		return errors.New("SHA256 does not match")
	}

	delete(rg.replicatedWrites, message.Key)

	return nil
}

func (rg *NodeReplicationGroup) AknowledgePrepare(message messages.ReplicationGroupWritePrepareMessage) error {
	err := rg.waitForQuorum()

	if err != nil {
		return err
	}

	rg.mutex.Lock()
	defer rg.mutex.Unlock()

	var replicatedWrite *ReplicationGroupReplicatedWrite
	var ok bool

	if replicatedWrite, ok = rg.replicatedWrites[message.Key]; !ok {
		return errors.New("no write found for key")
	}

	if message.Proposer == rg.cluster.node.Address() {
		return errors.New("proposer current node cannot be the proposer for prepare")
	}

	if message.Proposer != replicatedWrite.Proposer {
		return errors.New("proposer does not match")
	}

	if message.SHA256 != replicatedWrite.SHA256 {
		return errors.New("SHA256 does not match")
	}

	if replicatedWrite.PreparedAt != 0 {
		return errors.New("write already prepared")
	}

	replicatedWrite.PreparedAt = time.Now().Unix()

	return nil
}

func (rg *NodeReplicationGroup) AknowledgeWrite(message messages.ReplicationGroupWriteMessage) error {
	err := rg.waitForQuorum()

	if err != nil {
		return err
	}

	rg.mutex.Lock()
	defer rg.mutex.Unlock()

	if _, ok := rg.replicatedWrites[message.Key]; ok {
		return errors.New("write already replicated")
	}

	rg.replicatedWrites[message.Key] = &ReplicationGroupReplicatedWrite{
		Addresses:    message.Addresses,
		Data:         message.Data,
		Deadline:     message.Deadline,
		Key:          message.Key,
		Proposer:     message.Proposer,
		ReplicatedAt: time.Now().Unix(),
		SHA256:       message.SHA256,
	}

	return nil
}

// Commit a write to the ReplicationGroup. This will send a message to the other
// nodes in the group observe that the write was durably stored.
func (rg *NodeReplicationGroup) Commit(key string, sha256Hash string) error {
	err := rg.waitForQuorum()

	if err != nil {
		return err
	}

	if !rg.IsWriter() {
		return ErrNotWriterInReplicationGroup
	}

	addresses := make([]string, 0, len(rg.Members))

	for _, member := range rg.Members {
		addresses = append(addresses, member.address)
	}

	err = rg.send(messages.NodeMessage{
		Data: messages.ReplicationGroupWriteCommitMessage{
			Addresses: addresses,
			Key:       key,
			Proposer:  rg.cluster.node.Address(),
			SHA256:    sha256Hash,
		},
	})

	if err != nil {
		return err
	}

	return nil
}

func (rg *NodeReplicationGroup) ContainsAddresses(addresses []string) bool {
	for _, address := range addresses {
		found := false

		for _, member := range rg.Members {
			if member.address == address {
				found = true
				break
			}
		}

		if !found {
			return false
		}
	}

	return true
}

func (rg *NodeReplicationGroup) IsObserver() bool {
	for _, member := range rg.Members {
		if member.address == rg.cluster.node.Address() &&
			member.role == NodeReplicationGroupObserver {
			return true
		}
	}

	return false
}

func (rg *NodeReplicationGroup) IsWriter() bool {
	for _, member := range rg.Members {
		if member.address == rg.cluster.node.Address() &&
			member.role == NodeReplicationGroupWriter {
			return true
		}
	}

	return false
}

func (rg *NodeReplicationGroup) Prepare(key string, sha256Hash string) error {
	err := rg.waitForQuorum()

	if err != nil {
		return err
	}

	if !rg.IsWriter() {
		return ErrNotWriterInReplicationGroup
	}

	addresses := make([]string, 0, len(rg.Members))

	for _, member := range rg.Members {
		addresses = append(addresses, member.address)
	}

	err = rg.send(messages.NodeMessage{
		Data: messages.ReplicationGroupWritePrepareMessage{
			Addresses: addresses,
			Key:       key,
			Proposer:  rg.cluster.node.Address(),
			SHA256:    sha256Hash,
		},
	})

	if err != nil {
		return err
	}

	return nil
}

func (rg *NodeReplicationGroup) send(message messages.NodeMessage) error {
	rg.mutex.Lock()

	if len(rg.Members) == 0 {
		rg.mutex.Unlock()
		return errors.New("no addresses in replication group")
	}

	connections := make([]*NodeConnection, 0, len(rg.Members)-1)

	for _, member := range rg.Members {
		if member.address == rg.cluster.node.Address() {
			continue
		}

		var connection *NodeConnection
		var ok bool

		if connection, ok = rg.nodeConnections[member.address]; !ok {
			connection = NewNodeConnection(rg.cluster.node, member.address)
			rg.nodeConnections[member.address] = connection
		}

		connections = append(connections, connection)
	}

	rg.mutex.Unlock()

	wg := sync.WaitGroup{}
	errChan := make(chan error, len(connections))
	wg.Add(len(connections))

	for _, connection := range connections {
		go func(connection *NodeConnection) {
			defer wg.Done()

			response, err := connection.Send(message)

			if err != nil {
				log.Println("Failed to send message to node: ", err)
				errChan <- err
				return
			}

			if response == nil {
				errChan <- errors.New("nil response")
				return
			}

			if message, isError := response.(messages.ErrorMessage); isError {
				errChan <- errors.New(message.Message)
				return
			}
		}(connection)
	}

	wg.Wait()
	close(errChan)

	var errorOccurred error
	for err := range errChan {
		if err != nil {
			errorOccurred = err
			break
		}
	}

	if errorOccurred != nil {
		log.Println("At least one goroutine returned an error:", errorOccurred)

		return errors.New("failed to write to replication group")
	}

	return nil
}

func (rg *NodeReplicationGroup) SetMembers(members []NodeReplicationGroupMember) {
	rg.Members = members
	rg.quorumReached <- struct{}{}
	rg.quorumReachedAt = time.Now()
}

func (rg *NodeReplicationGroup) waitForQuorum() error {
	// If the quorum has already been reached, then return immediately.
	if !rg.formingQuorum && rg.quorumReachedAt.Add(NodeReplicationGroupQuromTTL).After(time.Now()) {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)

	defer func() {
		defer cancel()

		rg.formingQuorum = false
	}()

	rg.formingQuorum = true

	for {
		select {
		case <-ctx.Done():
			return errors.New("timeout waiting for replication group quorum")
		case <-rg.quorumReached:
			log.Println("Quorum reached")
			return nil
		default:
			if !rg.cluster.node.IsPrimary() {
				log.Println("Waiting for quorum to be reached", rg.cluster.node.Address(), rg.Members)
				// Contact the primary node to get placement
				// TODO: rg.cluster.node.Replica().GetReplicatonGroupPlacement()?
			} else {
				err := rg.cluster.node.ReplicationGroupManager.AssignReplicationGroups()

				if err != nil {
					return err
				}

				return nil
			}
		}
	}
}

// Write data to the replication group and wait for a quorum of nodes to
// acknowledge the write.
func (rg *NodeReplicationGroup) Write(key string, data []byte) error {
	err := rg.waitForQuorum()

	if err != nil {
		return err
	}

	if !rg.IsWriter() {
		return ErrNotWriterInReplicationGroup
	}

	// If there is only one node in the replication group, then the write can be
	// considered durably stored.
	if len(rg.Members) == 1 {
		return nil
	}

	sha256Hash := sha256.Sum256(data)

	addresses := make([]string, 0, len(rg.Members))

	for _, member := range rg.Members {
		addresses = append(addresses, member.address)
	}

	replicationMessage := messages.ReplicationGroupWriteMessage{
		Addresses: addresses,
		Data:      data,
		Deadline:  time.Now().Add(1*time.Second + storage.DefaultWriteInterval).Unix(),
		Key:       key,
		Proposer:  rg.cluster.node.Address(),
		SHA256:    hex.EncodeToString(sha256Hash[:]),
	}

	rg.pendingWrites[key] = replicationMessage

	err = rg.send(messages.NodeMessage{
		Data: replicationMessage,
	})

	if err != nil {
		return err
	}

	return nil
}
