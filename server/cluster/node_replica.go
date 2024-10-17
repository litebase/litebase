package cluster

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"runtime"
	"sync"
	"time"
)

type NodeReplica struct {
	cancel          context.CancelFunc
	context         context.Context
	Address         string
	Id              string
	node            *Node
	startMutex      *sync.RWMutex
	walSynchronizer NodeWalSynchronizer
}

func NewNodeReplica(node *Node) *NodeReplica {
	context, cancel := context.WithCancel(context.Background())

	replica := &NodeReplica{
		cancel:     cancel,
		context:    context,
		node:       node,
		startMutex: &sync.RWMutex{},
	}

	return replica
}

func (nr *NodeReplica) HandleMessage(message NodeMessage) (NodeMessage, error) {
	var responseMessage NodeMessage

	if message.Id == "broadcast" {
		err := nr.handleBroadcastMessage(message)

		if err != nil {
			return NodeMessage{}, err
		}

		return responseMessage, nil
	}

	switch message.Type {
	case "NodeConnectionMessage":
		responseMessage = NodeMessage{
			Id:   message.Id,
			Type: "NodeConnectionMessage",
		}
	}

	return responseMessage, nil
}

func (nr *NodeReplica) handleBroadcastMessage(message NodeMessage) error {
	switch message.Type {
	case "HeartbeatMessage":
		nr.node.PrimaryHeartbeat = time.Now()
	case "WALReplicationWriteMessage":
		// Verify the integrity of the WAL data
		sha256Hash := sha256.Sum256(message.Data.(WALReplicationWriteMessage).Data)

		if sha256Hash != message.Data.(WALReplicationWriteMessage).Sha256 {
			log.Println("Failed to verify WAL data integrity")
			return errors.New("failed to verify WAL data integrity")
		}

		err := nr.node.walSynchronizer.WriteAt(
			message.Data.(WALReplicationWriteMessage).DatabaseId,
			message.Data.(WALReplicationWriteMessage).BranchId,
			message.Data.(WALReplicationWriteMessage).Data,
			message.Data.(WALReplicationWriteMessage).Offset,
			message.Data.(WALReplicationWriteMessage).Sequence,
			message.Data.(WALReplicationWriteMessage).Timestamp,
		)

		if err != nil {
			log.Println("Failed to sync WAL data: ", err)
			return err
		}

	case "WALReplicationTruncateMessage":
		err := nr.node.walSynchronizer.Truncate(
			message.Data.(WALReplicationTruncateMessage).DatabaseId,
			message.Data.(WALReplicationTruncateMessage).BranchId,
			message.Data.(WALReplicationTruncateMessage).Size,
			message.Data.(WALReplicationTruncateMessage).Sequence,
			message.Data.(WALReplicationTruncateMessage).Timestamp,
		)

		if err != nil {
			log.Println("Failed to sync WAL data truncation: ", err)
			return err
		}
	}

	return nil
}

func (nr *NodeReplica) JoinCluster() error {
	httpClient := &http.Client{
		Timeout: 3 * time.Second,
	}

	url := fmt.Sprintf("http://%s/cluster/members", nr.node.PrimaryAddress())

	data := map[string]string{
		"address": nr.node.Address(),
		"group":   nr.node.cluster.Config.NodeType,
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

	encryptedHeader, err := nr.node.cluster.Auth.SecretsManager.Encrypt(
		nr.node.cluster.Config.Signature,
		nr.node.Address(),
	)

	if err != nil {
		log.Println(err)
		return err
	}

	request.Header.Set("X-Lbdb-Node", encryptedHeader)
	request.Header.Set("Content-Type", "application/json")

	response, err := httpClient.Do(request)

	if err != nil {
		log.Println("Failed to join cluster: ", err)
		return err
	}

	if response.StatusCode >= 400 {
		return errors.New("failed to join cluster")
	}

	return nil
}

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

	if nr.node.primaryAddress == "" {
		return nil
	}

	url := fmt.Sprintf("http://%s/cluster/members/%s", nr.node.primaryAddress, nr.node.Address())

	request, err := http.NewRequestWithContext(nr.node.context, "DELETE", url, nil)

	if err != nil {
		log.Println("Failed to leave cluster: ", err)
		return err
	}

	encryptedHeader, err := nr.node.cluster.Auth.SecretsManager.Encrypt(
		nr.node.cluster.Config.Signature,
		nr.node.Address(),
	)

	if err != nil {
		log.Println(err)
		return err
	}

	request.Header.Set("X-Lbdb-Node", encryptedHeader)

	_, err = httpClient.Do(request)

	if err != nil {
		log.Println("Failed to leave cluster: ", err)
		return err
	}

	return nil
}

func (nr *NodeReplica) Send(nodeMessage NodeMessage) (NodeMessage, error) {
	// return nr.primaryConnection.Send(nodeMessage)

	// log.Println("[SENDING]:", nodeMessage.Type)
	data := bytes.NewBuffer(nil)
	encoder := gob.NewEncoder(data)

	err := encoder.Encode(nodeMessage)

	if err != nil {
		log.Println("Failed to encode message: ", err)
		return NodeMessage{}, err
	}

	client := &http.Client{
		Timeout: 3 * time.Second,
	}

	request, err := http.NewRequestWithContext(nr.node.context, "POST", fmt.Sprintf("http://%s/cluster/primary", nr.node.PrimaryAddress()), data)

	if err != nil {
		log.Println("Failed to send message: ", err)
		return NodeMessage{}, err
	}

	encryptedHeader, err := nr.node.cluster.Auth.SecretsManager.Encrypt(
		nr.node.cluster.Config.Signature,
		nr.node.Address(),
	)

	if err != nil {
		log.Println(err)
		return NodeMessage{}, err
	}

	request.Header.Set("X-Lbdb-Node", encryptedHeader)
	request.Header.Set("Content-Type", "application/gob")

	response, err := client.Do(request)

	if err != nil {
		log.Println("Failed to send message: ", err)
		return NodeMessage{}, err
	}

	defer response.Body.Close()

	if response.StatusCode >= 400 {
		log.Println("Failed to send message: ", response.Status)
		runtime.Stack(nil, true)
		return NodeMessage{}, errors.New("failed to send message")
	}

	decoder := gob.NewDecoder(response.Body)

	var responseMessage NodeMessage

	err = decoder.Decode(&responseMessage)

	if err != nil {
		log.Println("Failed to decode response: ", err)
		return NodeMessage{}, err
	}

	return responseMessage, nil
}

func (nr *NodeReplica) SendWithStreamingResonse(nodeMessage NodeMessage) (chan NodeMessage, error) {
	if nr.node.PrimaryAddress() == "" {
		return nil, errors.New("Primary address is not set")
	}

	data := bytes.NewBuffer(nil)
	encoder := gob.NewEncoder(data)

	err := encoder.Encode(nodeMessage)

	if err != nil {
		log.Println("Failed to encode message: ", err)
		return nil, err
	}

	request, err := http.NewRequestWithContext(nr.node.context, "POST", fmt.Sprintf("http://%s/cluster/primary", nr.node.PrimaryAddress()), data)

	if err != nil {
		log.Println("Failed to send message: ", err)
		return nil, err
	}

	encryptedHeader, err := nr.node.cluster.Auth.SecretsManager.Encrypt(
		nr.node.cluster.Config.Signature,
		nr.node.Address(),
	)

	if err != nil {
		log.Println(err)
		return nil, err
	}

	request.Header.Set("X-Lbdb-Node", encryptedHeader)

	responses := make(chan NodeMessage)

	client := &http.Client{
		Timeout: 0,
		Transport: &http.Transport{
			DisableKeepAlives: true,
		},
	}

	response, err := client.Do(request)

	if err != nil {
		log.Println("Failed to send message: ", err)
		return nil, err
	}

	go func() {
		defer response.Body.Close()

		for {
			decoder := gob.NewDecoder(response.Body)

			var responseMessage NodeMessage

			err = decoder.Decode(&responseMessage)

			if err != nil {
				close(responses)
				return
			}

			// log.Println("[RECEIVED]:", responseMessage.Type)

			responses <- responseMessage
		}
	}()

	return responses, nil
}

// func (nr *NodeReplica) Start() (err error) {
// 	primaryAddress := nr.node.PrimaryAddress()

// 	if primaryAddress == "" || primaryAddress == nr.node.Address() {
// 		return nil
// 	}

// 	nr.primaryConnection = NewNodePrimaryConnection(
// 		primaryAddress,
// 		nr.databaseCheckpointer,
// 		nr.databaseWalSynchronizer,
// 	)

// 	go nr.primaryConnection.Open()

// 	return nil
// }

func (nr *NodeReplica) Stop() error {
	// if nr.primaryConnection != nil {
	// 	err := nr.primaryConnection.Close()

	// 	if err != nil {
	// 		return err
	// 	}
	// }

	nr.cancel()

	return nil
}
