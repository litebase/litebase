package cluster

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/litebase/litebase/pkg/cluster/messages"
)

type NodeReplica struct {
	cancel     context.CancelFunc
	Context    context.Context
	Address    string
	Id         string
	node       *Node
	startMutex *sync.RWMutex
}

// Create a new instance of a NodeReplica
func NewNodeReplica(node *Node) *NodeReplica {
	context, cancel := context.WithCancel(context.Background())

	replica := &NodeReplica{
		cancel:     cancel,
		Context:    context,
		node:       node,
		startMutex: &sync.RWMutex{},
	}

	return replica
}

// Join the cluster by informing the primary node
func (nr *NodeReplica) JoinCluster() error {
	httpClient := &http.Client{
		Timeout: 3 * time.Second,
	}

	url := fmt.Sprintf("http://%s/cluster/members", nr.node.PrimaryAddress())

	address, err := nr.node.Address()

	if err != nil {
		return fmt.Errorf("failed to get node address: %w", err)
	}

	data := map[string]string{
		"address": address,
		"id":      nr.node.ID,
	}

	jsonData, err := json.Marshal(data)

	if err != nil {
		return err
	}

	request, err := http.NewRequestWithContext(nr.node.context, "POST", url, bytes.NewBuffer(jsonData))

	if err != nil {
		log.Println("Failed to join cluster: ", err)
		return err
	}

	encryptedHeader, err := nr.node.Cluster.Auth.SecretsManager.Encrypt(
		nr.node.Cluster.Config.EncryptionKey,
		[]byte(address),
	)

	if err != nil {
		log.Println(err)
		return err
	}

	request.Header.Set("X-Lbdb-Node", string(encryptedHeader))
	request.Header.Set("Content-Type", "application/json")

	response, err := httpClient.Do(request)

	if err != nil {
		return err
	}

	if response.StatusCode >= 400 {
		return errors.New("failed to join cluster")
	}

	return nil
}

// Leave the cluster by informing the primary node
func (nr *NodeReplica) LeaveCluster() error {
	// Check if the context is canceled
	if nr.node.context.Err() != nil {
		return fmt.Errorf("node context is canceled")
	}

	httpClient := &http.Client{
		Timeout: 3 * time.Second,
	}

	if nr.node == nil {
		return nil
	}

	if nr.node.PrimaryAddress() == "" {
		return nil
	}

	address, err := nr.node.Address()

	if err != nil {
		return fmt.Errorf("failed to get node address: %w", err)
	}

	url := fmt.Sprintf("http://%s/cluster/members/%s", nr.node.PrimaryAddress(), address)

	request, err := http.NewRequestWithContext(nr.node.context, "DELETE", url, nil)

	if err != nil {
		log.Println("Failed to leave cluster: ", err)
		return err
	}

	encryptedHeader, err := nr.node.Cluster.Auth.SecretsManager.Encrypt(
		nr.node.Cluster.Config.EncryptionKey,
		[]byte(address),
	)

	if err != nil {
		log.Println(err)
		return err
	}

	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Lbdb-Node", string(encryptedHeader))
	request.Header.Set("X-Lbdb-Node-Timestamp", fmt.Sprintf("%d", time.Now().UTC().UnixNano()))

	resp, err := httpClient.Do(request)

	if err != nil {
		log.Println("Failed to leave cluster: ", err)
		return err
	}

	if resp.StatusCode >= 400 {
		return errors.New("failed to leave cluster")
	}

	return nil
}

// Send a message from the replica to the primary node
func (nr *NodeReplica) Send(message messages.NodeMessage) (messages.NodeMessage, error) {
	data := bytes.NewBuffer(nil)
	encoder := gob.NewEncoder(data)
	address, err := nr.node.Address()

	if err != nil {
		return messages.NodeMessage{}, fmt.Errorf("failed to get node address: %w", err)
	}

	err = encoder.Encode(message)

	if err != nil {
		return messages.NodeMessage{}, err
	}

	client := &http.Client{
		Timeout: 3 * time.Second,
	}

	request, err := http.NewRequestWithContext(
		nr.node.context, "POST",
		fmt.Sprintf("http://%s/cluster/primary", nr.node.PrimaryAddress()),
		data,
	)

	if err != nil {
		slog.Error("Failed to send message", "error", err)
		return messages.NodeMessage{}, err
	}

	encryptedHeader, err := nr.node.Cluster.Auth.SecretsManager.Encrypt(
		nr.node.Cluster.Config.EncryptionKey,
		[]byte(address),
	)

	if err != nil {
		slog.Error("Failed to encrypt header", "error", err)
		return messages.NodeMessage{}, err
	}

	request.Header.Set("X-Lbdb-Node", string(encryptedHeader))
	request.Header.Set("X-Lbdb-Node-Timestamp", fmt.Sprintf("%d", time.Now().UTC().UnixNano()))
	request.Header.Set("Content-Type", "application/gob")

	response, err := client.Do(request)

	if err != nil {
		slog.Error("Failed to send message", "error", err)
		return messages.NodeMessage{}, err
	}

	defer response.Body.Close()

	if response.StatusCode >= 400 {
		slog.Error("Failed to send message", "status", response.Status)
		return messages.NodeMessage{}, errors.New("failed to send message")
	}

	decoder := gob.NewDecoder(response.Body)

	var responseMessage messages.NodeMessage

	err = decoder.Decode(&responseMessage)

	if err != nil {
		slog.Error("Failed to decode response", "error", err)
		return messages.NodeMessage{}, err
	}

	return responseMessage, nil
}

// Stop the replica context
func (nr *NodeReplica) Stop() error {
	nr.cancel()

	return nil
}
