package cluster

import (
	"errors"
	"fmt"
	"litebase/internal/config"
	"litebase/server/cluster/messages"
	"log"
	"net"
	"net/http"
	"sync"
	"time"
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
	errorMap := np.Publish(messages.NodeMessage{
		Data: messages.HeartbeatMessage{
			Address: np.node.Address(),
			ID:      []byte("broadcast"),
		},
	})

	experiencedError := false

	for address, err := range errorMap {
		if err != nil {
			log.Println("Failed to send heartbeat message: ", err)
			experiencedError = true

			// Remove the address file
			np.ValidateReplica(address)
		}
	}

	if experiencedError {
		return errors.New("failed to send heartbeat message")
	}

	return nil
}

// Publish a message to the replica nodes.
func (np *NodePrimary) Publish(message messages.NodeMessage) map[string]error {
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
	errorMap := map[string]error{}
	errorsMutex := sync.Mutex{}

	wg.Add(len(connections))

	for _, connection := range connections {
		go func(node *NodeConnection) {
			defer wg.Done()

			_, err := connection.Send(message)

			if err != nil {
				log.Println("Failed to send message to node: ", err)
			}

			errorsMutex.Lock()
			errorMap[connection.Address] = err
			errorsMutex.Unlock()
		}(connection)
	}

	wg.Wait()

	return errorMap
}

// Shutdown the primary node.
func (np *NodePrimary) Shutdown() {
	np.mutex.Lock()
	defer np.mutex.Unlock()

	for _, connection := range np.nodeConnections {
		connection.Close()
	}
}

// Validate that the replica node is still connected by trying to reach it. If
// the replica cannot be reached, remove replicas node the file from storage.
func (np *NodePrimary) ValidateReplica(address string) error {
	request, err := http.NewRequest("GET", fmt.Sprintf("http://%s/health", address), nil)

	if err != nil {
		log.Println("Failed to validate replica: ", err)
		return err
	}

	client := http.Client{
		Timeout: 1,
		Transport: &http.Transport{
			DialContext: (&net.Dialer{
				Timeout: 1 * time.Second, // Timeout for establishing a connection
			}).DialContext,
		},
	}

	np.node.setInternalHeaders(request)

	response, err := client.Do(request)

	if err != nil {
		log.Println("Failed to validate replica: ", err)
	}

	if response != nil && response.StatusCode == http.StatusOK {
		return nil
	}

	if err := np.node.cluster.RemoveMember(address); err != nil {
		log.Println("Failed to remove replica: ", err)

		return err
	}

	return nil
}
