package cluster

import (
	"crypto/sha256"
	"litebase/server/cluster/messages"
)

// The NodeReplicator is responsible for distributing database WAL changes to other
// nodes in the cluster.
type NodeWalReplicator struct {
	node *Node
}

// Create a new instance of a NodeReplicator.
func NewNodeReplicator(node *Node) *NodeWalReplicator {
	return &NodeWalReplicator{
		node: node,
	}
}

// Replicate a truncation of the WAL to all of the other nodes in the cluster.
func (nr *NodeWalReplicator) Truncate(
	databaseId,
	branchId string,
	size, sequence, timestamp int64,
) error {
	if !nr.node.IsPrimary() {
		return nil
	}

	return nr.node.Primary().Publish(messages.WALReplicationTruncateMessage{
		BranchId:   branchId,
		DatabaseId: databaseId,
		ID:         []byte("broadcast"),
		Sequence:   sequence,
		Size:       size,
		Timestamp:  timestamp,
	})
}

// Replicate a write to the WAL to all of the other nodes in the cluster.
func (nr *NodeWalReplicator) WriteAt(databaseId, branchId string, p []byte, off, sequence, timestamp int64) error {
	if !nr.node.IsPrimary() {
		return nil
	}

	sha256Hash := sha256.Sum256(p)

	return nr.node.Primary().Publish(messages.WALReplicationWriteMessage{
		BranchId:   branchId,
		DatabaseId: databaseId,
		Data:       p,
		ID:         []byte("broadcast"),
		Offset:     off,
		Sequence:   sequence,
		Sha256:     sha256Hash,
		Timestamp:  timestamp,
	})
}
