package cluster

import (
	"crypto/sha256"
	"errors"
	"litebase/server/cluster/messages"
)

// The NodeReplicator is responsible for distributing database WAL changes to other
// nodes in the cluster.
type NodeWALReplicator struct {
	node *Node
}

// Create a new instance of a NodeReplicator.
func NewNodeWALReplicator(node *Node) *NodeWALReplicator {
	return &NodeWALReplicator{
		node: node,
	}
}

// Replicate a truncation of the WAL to all of the other nodes in the cluster.
func (nr *NodeWALReplicator) Truncate(
	databaseId,
	branchId string,
	size, sequence, timestamp int64,
) error {
	if !nr.node.IsPrimary() {
		return errors.New("node is not primary")
	}

	errorMap := nr.node.Primary().Publish(messages.NodeMessage{
		Data: messages.WALReplicationTruncateMessage{
			BranchId:   branchId,
			DatabaseId: databaseId,
			ID:         []byte("broadcast"),
			Sequence:   sequence,
			Size:       size,
			Timestamp:  timestamp,
		},
	})

	for _, err := range errorMap {
		if err != nil {
			return errors.New("failed to truncate WAL")
		}
	}

	return nil
}

// Replicate a write to the WAL to all of the other nodes in the cluster.
func (nr *NodeWALReplicator) WriteAt(databaseId, branchId string, p []byte, off, sequence, timestamp int64) error {
	if !nr.node.IsPrimary() {
		return errors.New("node is not primary")
	}

	sha256Hash := sha256.Sum256(p)

	errorMap := nr.node.Primary().Publish(messages.NodeMessage{
		Data: messages.WALReplicationWriteMessage{
			BranchId:   branchId,
			DatabaseId: databaseId,
			Data:       p,
			ID:         []byte("broadcast"),
			Offset:     off,
			Sequence:   sequence,
			Sha256:     sha256Hash,
			Timestamp:  timestamp,
		},
	})

	for _, err := range errorMap {
		if err != nil {
			return errors.New("failed to write to WAL")
		}
	}

	return nil
}
