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

func (np *NodePrimary) HandleMessage(message messages.NodeMessage) (messages.NodeMessage, error) {
	var responseMessage messages.NodeMessage

	switch message.Type() {
	case "HeartbeatMessage":
		isPrimary := np.node.VerifyPrimaryStatus()

		if !isPrimary {
			responseMessage = messages.ErrorMessage{
				ID:      message.Id(),
				Message: "Node is not primary",
			}
		} else {
			responseMessage = messages.ErrorMessage{
				ID:      message.Id(),
				Message: "Node is the primary",
			}
		}
	case "NodeConnectionMessage":
		responseMessage = messages.NodeConnectionMessage{
			ID: message.Id(),
		}
	case "QueryMessage":
		responseMessage = np.handleQueryMessage(message)
	default:
		log.Println("Invalid message type: ", message.Type)
		responseMessage = messages.ErrorMessage{
			ID:      message.Id(),
			Message: "invalid message type",
		}
	}

	if responseMessage != nil {
		return responseMessage, nil
	}

	return nil, nil
}

func (np *NodePrimary) handleQueryMessage(message messages.NodeMessage) messages.NodeMessage {
	query, err := np.node.queryBuilder.Build(
		message.(messages.QueryMessage).AccessKeyId,
		message.(messages.QueryMessage).DatabaseHash,
		message.(messages.QueryMessage).DatabaseId,
		message.(messages.QueryMessage).BranchId,
		message.(messages.QueryMessage).Statement,
		message.(messages.QueryMessage).Parameters,
		message.(messages.QueryMessage).Id(),
	)

	if err != nil {
		log.Println("Failed to build query: ", err)

		return messages.ErrorMessage{
			Message: err.Error(),
			ID:      message.Id(),
		}
	}

	var response NodeQueryResponse

	err = query.Resolve(response)

	if err != nil {
		log.Println("Failed to process query message: ", err)
		return messages.ErrorMessage{
			Message: err.Error(),
			ID:      message.Id(),
		}
	}

	return messages.QueryMessageResponse{
		Changes:         response.Changes(),
		Columns:         response.Columns(),
		ID:              message.Id(),
		LastInsertRowID: response.LastInsertRowId(),
		Latency:         response.Latency(),
		RowCount:        response.RowCount(),
		Rows:            response.Rows(),
	}
}

// Send the heatbeat message to the replica nodes.
func (np *NodePrimary) Heartbeat() error {
	return np.Publish(messages.HeartbeatMessage{
		ID: []byte("broadcast"),
	})
}

func (np *NodePrimary) Publish(nodeMessage messages.NodeMessage) error {
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

	wg.Add(len(connections))

	for _, connection := range connections {
		go func(node *NodeConnection) {
			defer wg.Done()

			_, err := connection.Send(nodeMessage)

			if err != nil {
				log.Println("Failed to send message to node: ", err)
			}
		}(connection)
	}

	wg.Wait()

	return nil
}

func (np *NodePrimary) Shutdown() {
	np.mutex.Lock()
	defer np.mutex.Unlock()

	for _, connection := range np.nodeConnections {
		connection.Close()
	}
}
