package cluster

import (
	"litebase/internal/config"
	"litebase/server/cluster/messages"
	"log"
	"sync"
)

type NodePrimary struct {
	mutex           *sync.RWMutex
	node            *Node
	nodeConnections map[string]*NodeConnection
}

func NewNodePrimary(node *Node) *NodePrimary {
	primary := &NodePrimary{
		mutex:           &sync.RWMutex{},
		node:            node,
		nodeConnections: map[string]*NodeConnection{},
	}

	return primary
}

// Send the heatbeat message to the replica nodes.
func (np *NodePrimary) Heartbeat() error {
	return np.Publish(messages.NodeMessage{
		Data: messages.HeartbeatMessage{
			Address: np.node.Address(),
			ID:      []byte("broadcast"),
		},
	})
}

// Publish a message to the replica nodes.
func (np *NodePrimary) Publish(message messages.NodeMessage) error {
	var nodes []*NodeIdentifier

	if np.node == nil || np.node.cluster == nil {
		return nil
	}

	if np.node.cluster.Config.NodeType == config.NodeTypeQuery {
		nodes = np.node.cluster.OtherQueryNodes()
	} else if np.node.cluster.Config.NodeType == config.NodeTypeStorage {
		nodes = np.node.cluster.OtherStorageNodes()
	}

	if len(nodes) == 0 {
		return nil
	}

	np.mutex.Lock()
	connections := make([]*NodeConnection, len(nodes))

	for i, node := range nodes {
		var connection *NodeConnection
		var ok bool

		if connection, ok = np.nodeConnections[node.String()]; !ok {
			connection = NewNodeConnection(np.node, node.String())
			np.nodeConnections[node.String()] = connection
			connections[i] = np.nodeConnections[node.String()]
		} else {
			connections[i] = connection
		}
	}

	np.mutex.Unlock()

	wg := sync.WaitGroup{}
	errors := make([]error, 0)

	wg.Add(len(connections))

	for _, connection := range connections {
		go func(node *NodeConnection) {
			defer wg.Done()

			_, err := connection.Send(message)

			if err != nil {
				log.Println("Failed to send message to node: ", err)
			}

			errors = append(errors, err)
		}(connection)
	}

	wg.Wait()

	for _, err := range errors {
		if err != nil {
			return err
		}
	}

	return nil
}

// Shutdown the primary node.
func (np *NodePrimary) Shutdown() {
	np.mutex.Lock()
	defer np.mutex.Unlock()

	for _, connection := range np.nodeConnections {
		connection.Close()
	}
}
