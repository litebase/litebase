package cluster

import (
	"errors"
	"litebase/server/cluster/messages"
	"log"
	"sync"
)

type NodeReplicationGroupMember struct {
	address string
	role    NodeReplicationGroupRole
}

type NodeReplicationGroupManager struct {
	assignments       [][]NodeReplicationGroupMember
	node              *Node
	mutex             *sync.RWMutex
	ReplicationGroups []*NodeReplicationGroup
}

func NewNodeReplicationGroupManager(node *Node) *NodeReplicationGroupManager {
	return &NodeReplicationGroupManager{
		node:  node,
		mutex: &sync.RWMutex{},
	}
}

func (nrgm *NodeReplicationGroupManager) AssignReplicationGroups() error {
	allNodes := nrgm.node.cluster.AllStorageNodes()

	if len(allNodes) == 0 {
		return errors.New("no storage nodes in cluster")
	}

	// Divide the nodes into replication groups of three nodes each.
	groupIndex := 0
	nrgm.assignments = make([][]NodeReplicationGroupMember, 0)

	if len(allNodes) > 0 {
		nrgm.assignments = append(nrgm.assignments, []NodeReplicationGroupMember{})
	}

	for _, node := range allNodes {
		if len(nrgm.assignments) == 0 || len(nrgm.assignments[groupIndex]) == 3 {
			nrgm.assignments = append(nrgm.assignments, []NodeReplicationGroupMember{})
			groupIndex++
		}

		nrgm.assignments[groupIndex] = append(nrgm.assignments[groupIndex], NodeReplicationGroupMember{
			role:    NodeReplicationGroupWriter,
			address: node.String(),
		})
	}

	nrgm.borrowMembersToCompleteGroups()

	for _, group := range nrgm.assignments {
		for _, member := range group {
			if member.address == nrgm.node.Address() {
				if member.role == NodeReplicationGroupWriter {
					nrgm.node.ReplicationGroupManager.WriterGroup().SetMembers(group)
				} else {
					replicationGroup := NewNodeReplicationGroup(nrgm.node.cluster)
					replicationGroup.Members = group
					nrgm.ReplicationGroups = append(nrgm.ReplicationGroups, replicationGroup)
				}
			}
		}
	}

	// Send assignments to all storage nodes
	var assignments [][]messages.ReplicationGroupAssignment

	for _, group := range nrgm.assignments {
		var assignment []messages.ReplicationGroupAssignment

		for _, member := range group {
			assignment = append(assignment, messages.ReplicationGroupAssignment{
				Address: member.address,
				Role:    string(member.role),
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

func (nrgm *NodeReplicationGroupManager) borrowMembersToCompleteGroups() {
	if len(nrgm.assignments) <= 1 {
		return
	}

	for i, group := range nrgm.assignments {
		if i == 0 || len(group) == 3 {
			continue
		}

		borrowedIndex := 0

		for len(group) < 3 {
			borrowedMember := nrgm.assignments[0][borrowedIndex]

			group = append(group, NodeReplicationGroupMember{
				address: borrowedMember.address,
				role:    NodeReplicationGroupObserver,
			})

			borrowedIndex++
		}

		nrgm.assignments[i] = group
	}
}

func (nrgm *NodeReplicationGroupManager) clearNonWritingGroups() {
	for i, group := range nrgm.ReplicationGroups {
		for _, member := range group.Members {
			if member.address == nrgm.node.Address() && member.role != NodeReplicationGroupWriter {
				nrgm.ReplicationGroups = append(nrgm.ReplicationGroups[:i], nrgm.ReplicationGroups[i+1:]...)
			}
		}
	}
}

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
func (nrgm *NodeReplicationGroupManager) HandledReplcationGroupAssignmentMessage(message messages.ReplicationGroupAssignmentMessage) error {
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
				address: assignment.Address,
				role:    NodeReplicationGroupRole(assignment.Role),
			})
		}

		if len(members) != 0 && groupAssignment.role == NodeReplicationGroupWriter {
			nrgm.node.ReplicationGroupManager.WriterGroup().SetMembers(members)
		} else {
			replicationGroup := NewNodeReplicationGroup(nrgm.node.cluster)
			replicationGroup.Members = members
			nrgm.ReplicationGroups = append(nrgm.ReplicationGroups, replicationGroup)
		}
	}

	return nil
}

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
