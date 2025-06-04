package cluster

import (
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/litebase/litebase/server/cluster/messages"
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
	address, err := np.node.Address()

	if err != nil {
		return fmt.Errorf("failed to get node address: %w", err)
	}

	_, errorMap := np.Publish(messages.NodeMessage{
		Data: messages.HeartbeatMessage{
			Address: address,
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
func (np *NodePrimary) Publish(message messages.NodeMessage) (map[string]any, map[string]error) {
	if np.node == nil || np.node.Cluster == nil {
		return nil, nil
	}

	nodes := np.node.Cluster.OtherNodes()

	if len(nodes) == 0 {
		return nil, nil
	}

	np.mutex.Lock()
	connections := make([]*NodeConnection, len(nodes))

	for i, node := range nodes {
		var connection *NodeConnection
		var ok bool

		if connection, ok = np.nodeConnections[node.Address]; !ok {
			connection = NewNodeConnection(np.node, node.Address)
			np.nodeConnections[node.Address] = connection
			connections[i] = np.nodeConnections[node.Address]
		} else {
			connections[i] = connection
		}
	}

	np.mutex.Unlock()

	wg := sync.WaitGroup{}
	responseMap := map[string]any{}
	errorMap := map[string]error{}
	responseMutex := sync.Mutex{}

	wg.Add(len(connections))

	for _, connection := range connections {
		go func(node *NodeConnection) {
			defer wg.Done()

			response, err := connection.Send(message)

			responseMutex.Lock()

			if err != nil {
				errorMap[connection.Address] = err
				return
			}

			responseMap[connection.Address] = response

			responseMutex.Unlock()
		}(connection)
	}

	wg.Wait()

	return responseMap, errorMap
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
		slog.Debug("Failed to validate replica", "error", err)
	}

	if response != nil && response.StatusCode == http.StatusOK {
		return nil
	}

	if err := np.node.Cluster.RemoveMember(address, true); err != nil {
		slog.Error("Failed to remove replica", "error", err)

		return err
	}

	return nil
}
