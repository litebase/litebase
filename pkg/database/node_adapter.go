package database

import (
	"github.com/litebase/litebase/pkg/cluster"
	"github.com/litebase/litebase/pkg/cluster/messages"
)

// NodeAdapter adapts cluster.Node to implement storage.NodePublisher interface
type NodeAdapter struct {
	node *cluster.Node
}

// NewNodeAdapter creates a new NodeAdapter
func NewNodeAdapter(node *cluster.Node) *NodeAdapter {
	return &NodeAdapter{node: node}
}

// Publish implements storage.NodePublisher
func (na *NodeAdapter) Publish(message any) (map[string]any, map[string]error) {
	// Convert our message to NodeMessage format
	nodeMessage := messages.NodeMessage{
		Data: message,
	}

	// Only primary nodes can publish to replicas
	if na.node.IsPrimary() && na.node.Primary() != nil {
		resultMap, errorMap := na.node.Primary().Publish(nodeMessage)

		// Convert map[string]any to map[string]any and unwrap NodeMessage responses
		convertedResultMap := make(map[string]any)

		for k, v := range resultMap {
			// Unwrap NodeMessage responses
			if nodeMsg, ok := v.(messages.NodeMessage); ok {
				convertedResultMap[k] = nodeMsg.Data
			} else {
				convertedResultMap[k] = v
			}
		}

		return convertedResultMap, errorMap
	}

	return nil, nil
}

// IsReplica implements storage.NodePublisher
func (na *NodeAdapter) IsReplica() bool {
	return na.node.IsReplica()
}

// IsPrimary implements storage.NodePublisher
func (na *NodeAdapter) IsPrimary() bool {
	return na.node.IsPrimary()
}
