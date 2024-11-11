package cluster

import (
	"errors"
	"litebase/server/cluster/messages"
	"log"
	"sync"
)

type NodeReplicationGroupMember struct {
	Address string
	Role    NodeReplicationGroupRole
}

type NodeReplicationGroupManager struct {
	Assignments       [][]NodeReplicationGroupMember
	node              *Node
	mutex             *sync.RWMutex
	ReplicationGroups []*NodeReplicationGroup
}

// Create a new instance of the NodeReplicationGroupManager.
func NewNodeReplicationGroupManager(node *Node) *NodeReplicationGroupManager {
	return &NodeReplicationGroupManager{
		node:  node,
		mutex: &sync.RWMutex{},
	}
}

// Assign replication groups to all storage nodes in the cluster.
func (nrgm *NodeReplicationGroupManager) AssignReplicationGroups() error {
	if !nrgm.node.IsPrimary() {
		return errors.New("node replicatication group manager node is not primary, cannot assign replication groups")
	}

	allNodes := nrgm.node.cluster.AllStorageNodes()

	if len(allNodes) == 0 {
		return errors.New("no storage nodes in cluster")
	}

	// Divide the nodes into replication groups of three nodes each.
	groupIndex := 0
	nrgm.Assignments = make([][]NodeReplicationGroupMember, 0)

	if len(allNodes) > 0 {
		nrgm.Assignments = append(nrgm.Assignments, []NodeReplicationGroupMember{})
	}

	for _, node := range allNodes {
		if len(nrgm.Assignments) == 0 || len(nrgm.Assignments[groupIndex]) == 3 {
			nrgm.Assignments = append(nrgm.Assignments, []NodeReplicationGroupMember{})
			groupIndex++
		}

		nrgm.Assignments[groupIndex] = append(nrgm.Assignments[groupIndex], NodeReplicationGroupMember{
			Address: node.String(),
			Role:    NodeReplicationGroupWriter,
		})
	}

	nrgm.borrowMembersToCompleteGroups()

	// Assign the members for the current node
	for _, group := range nrgm.Assignments {
		for _, member := range group {
			if member.Address == nrgm.node.Address() {
				if member.Role == NodeReplicationGroupWriter {
					nrgm.node.ReplicationGroupManager.WriterGroup().SetMembers(group)
				} else {
					replicationGroup := NewNodeReplicationGroup(nrgm.node.cluster)
					replicationGroup.Members = group
					nrgm.ReplicationGroups = append(nrgm.ReplicationGroups, replicationGroup)
				}
			}
		}
	}

	// Prepare assignments to be sent to all storage nodes
	var assignments [][]messages.ReplicationGroupAssignment

	for _, group := range nrgm.Assignments {
		var assignment []messages.ReplicationGroupAssignment

		for _, member := range group {
			assignment = append(assignment, messages.ReplicationGroupAssignment{
				Address: member.Address,
				Role:    string(member.Role),
			})
		}

		assignments = append(assignments, assignment)
	}

	err := nrgm.node.Primary().Publish(messages.NodeMessage{
		Data: messages.ReplicationGroupAssignmentMessage{
			ID:          []byte("broadcast"),
			Assignments: assignments,
		},
	})

	if err != nil {
		log.Println("Failed to publish replication group assignments: ", err)
		return err
	}

	return nil
}

// Borrow members from the first replication group to form a complete group.
func (nrgm *NodeReplicationGroupManager) borrowMembersToCompleteGroups() {
	if len(nrgm.Assignments) <= 1 {
		return
	}

	for i, group := range nrgm.Assignments {
		if i == 0 || len(group) == 3 {
			continue
		}

		borrowedIndex := 0

		for len(group) < 3 {
			borrowedMember := nrgm.Assignments[0][borrowedIndex]

			group = append(group, NodeReplicationGroupMember{
				Address: borrowedMember.Address,
				Role:    NodeReplicationGroupObserver,
			})

			borrowedIndex++
		}

		nrgm.Assignments[i] = group
	}
}

// Clear replication groups that are not writing groups.
func (nrgm *NodeReplicationGroupManager) clearNonWritingGroups() {
	for i, group := range nrgm.ReplicationGroups {
		for _, member := range group.Members {
			if member.Address == nrgm.node.Address() && member.Role != NodeReplicationGroupWriter {
				nrgm.ReplicationGroups = append(nrgm.ReplicationGroups[:i], nrgm.ReplicationGroups[i+1:]...)
			}
		}
	}
}

// Find the replication group for the given addresses.
func (nrgm *NodeReplicationGroupManager) FindForAddresses(addresses []string) (*NodeReplicationGroup, error) {
	nrgm.mutex.RLock()
	defer nrgm.mutex.RUnlock()

	for _, group := range nrgm.ReplicationGroups {
		if group.ContainsAddresses(addresses) {
			return group, nil
		}
	}

	return nil, errors.New("no replication group found for addresses")
}

// Handle the replication group assignment message from the primary node.
func (nrgm *NodeReplicationGroupManager) HandleReplcationGroupAssignmentMessage(message messages.ReplicationGroupAssignmentMessage) error {
	nrgm.clearNonWritingGroups()

	var assignmentGroups []struct {
		groupIndex int
		role       NodeReplicationGroupRole
	}

	var members []NodeReplicationGroupMember

	for i, assignments := range message.Assignments {
		for _, assignment := range assignments {
			if assignment.Address == nrgm.node.Address() {
				assignmentGroups = append(assignmentGroups, struct {
					groupIndex int
					role       NodeReplicationGroupRole
				}{
					groupIndex: i,
					role:       NodeReplicationGroupRole(assignment.Role),
				})
			}
		}
	}

	for _, groupAssignment := range assignmentGroups {
		for _, assignment := range message.Assignments[groupAssignment.groupIndex] {
			members = append(members, NodeReplicationGroupMember{
				Address: assignment.Address,
				Role:    NodeReplicationGroupRole(assignment.Role),
			})
		}

		if len(members) != 0 && groupAssignment.role == NodeReplicationGroupWriter {
			nrgm.WriterGroup().SetMembers(members)
		} else {
			replicationGroup := NewNodeReplicationGroup(nrgm.node.cluster)
			replicationGroup.Members = members
			nrgm.ReplicationGroups = append(nrgm.ReplicationGroups, replicationGroup)
		}
	}

	return nil
}

// Get the writer group for the node or create a new one.
func (nrgm *NodeReplicationGroupManager) WriterGroup() *NodeReplicationGroup {
	nrgm.mutex.Lock()
	defer nrgm.mutex.Unlock()

	for _, group := range nrgm.ReplicationGroups {
		if group.writeGroup {
			return group
		}
	}

	group := NewNodeReplicationGroup(nrgm.node.cluster)
	group.writeGroup = true
	nrgm.ReplicationGroups = append(nrgm.ReplicationGroups, group)

	return group
}
