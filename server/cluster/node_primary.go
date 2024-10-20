package cluster

import (
	"litebase/internal/config"
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

func (np *NodePrimary) HandleMessage(message NodeMessage) (NodeMessage, error) {
	var responseMessage NodeMessage

	switch message.Type {
	case "HeartbeatMessage":
		isPrimary := np.node.VerifyPrimaryStatus()

		if !isPrimary {
			responseMessage = NodeMessage{
				Id:   message.Id,
				Type: "ErrorMessage",
				Data: ErrorMessage{
					Message: "Node is not primary",
				},
			}
		} else {
			responseMessage = NodeMessage{
				Id:   message.Id,
				Type: "ErrorMessage",
				Data: ErrorMessage{
					Message: "Node is the primary",
				},
			}
		}
	case "NodeConnectionMessage":
		responseMessage = NodeMessage{
			Id:   message.Id,
			Type: "NodeConnectionMessage",
		}
	case "QueryMessage":
		responseMessage = np.handleQueryMessage(message)
	default:
		log.Println("Invalid message type: ", message.Type)
		responseMessage = NodeMessage{
			Error: "invalid message type",
			Id:    message.Id,
			Type:  "Error",
		}
	}

	if responseMessage != (NodeMessage{}) {
		return responseMessage, nil
	}

	return NodeMessage{}, nil
}

func (np *NodePrimary) handleQueryMessage(message NodeMessage) NodeMessage {
	query, err := np.node.queryBuilder.Build(
		message.Data.(QueryMessage).AccessKeyId,
		message.Data.(QueryMessage).DatabaseHash,
		message.Data.(QueryMessage).DatabaseId,
		message.Data.(QueryMessage).BranchId,
		message.Data.(QueryMessage).Statement,
		message.Data.(QueryMessage).Parameters,
		message.Data.(QueryMessage).Id,
	)

	if err != nil {
		log.Println("Failed to build query: ", err)

		return NodeMessage{
			Error: err.Error(),
			Id:    message.Id,
			Type:  "Error",
		}
	}

	// TODO: Implement this, needs to be an instance of query.QueryResponse
	var response NodeQueryResponse

	err = query.Resolve(response)

	if err != nil {
		log.Println("Failed to process query message: ", err)
		return NodeMessage{
			Error: err.Error(),
			Id:    message.Id,
			Type:  "Error",
		}
	}

	jsonData, _ := response.ToJSON()

	return NodeMessage{
		Id:   message.Id,
		Type: "QueryMessageResponse",
		Data: jsonData,
		// Data: QueryMessageResponse{
		// 	Changes:         response.Changes,
		// 	Columns:         response.Columns(),
		// 	Latency:   response.Latency(),
		// 	LastInsertRowID: response.LastInsertRowId(),
		// 	RowCount:        response.RowCount(),
		// 	Rows:            response.Rows(),
		// },
	}
}

// Send the heatbeat message to the replica nodes.
func (np *NodePrimary) Heartbeat() error {
	return np.Publish(NodeMessage{
		Id:   "broadcast",
		Type: "HeartbeatMessage",
	})
}

func (np *NodePrimary) Publish(nodeMessage NodeMessage) error {
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
