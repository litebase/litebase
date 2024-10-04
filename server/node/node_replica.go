package node

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"litebase/internal/config"
	"litebase/server/auth"
	"log"
	"net/http"
	"runtime"
	"sync"
	"time"

	"github.com/klauspost/compress/s2"
)

type NodeReplica struct {
	cancel                  context.CancelFunc
	context                 context.Context
	Address                 string
	databaseCheckpointer    NodeReplicaCheckpointer
	databaseWalSynchronizer NodeDatabaseWalSynchronizer
	Id                      string
	node                    *NodeInstance
	startMutex              *sync.RWMutex
}

func NewNodeReplica(node *NodeInstance) *NodeReplica {
	context, cancel := context.WithCancel(context.Background())

	replica := &NodeReplica{
		cancel:                  cancel,
		context:                 context,
		databaseCheckpointer:    node.databaseCheckpointer,
		databaseWalSynchronizer: node.databaseWalSynchronizer,
		node:                    node,
		startMutex:              &sync.RWMutex{},
	}

	// if Node().PrimaryAddress() != "" {
	// 	err := replica.Start()

	// 	if err != nil {
	// 		log.Println(err)
	// 	}
	// }

	// go func() {
	// 	ticker := time.NewTicker(1000 * time.Millisecond)

	// 	for {
	// 		select {
	// 		case <-Node().Context().Done():
	// 		case <-replica.context.Done():
	// 			return
	// 		case <-ticker.C:
	// 			// if replica.primaryConnection == nil {
	// 			// 	replica.Start()
	// 			// 	return
	// 			// }

	// 			// if !replica.primaryConnection.Connected() {
	// 			// 	replica.primaryConnection.Close()
	// 			// 	replica.Start()
	// 			// }
	// 		}
	// 	}
	// }()

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

	return NodeMessage{}, nil
}

func (nr *NodeReplica) handleBroadcastMessage(message NodeMessage) error {
	switch message.Type {
	case "HeartbeatMessage":
		Node().PrimaryHeartbeat = time.Now()
	case "WALCheckpointMessage":
		err := nr.databaseCheckpointer.CheckpointReplica(
			message.Data.(WALCheckpointMessage).DatabaseUuid,
			message.Data.(WALCheckpointMessage).BranchUuid,
			message.Data.(WALCheckpointMessage).Timestamp,
		)

		if err != nil {
			log.Println("Failed to checkpoint WAL: ", err)
			return err
		}
	case "WALReplicationMessage":
		decompressedData, err := s2.Decode(nil, message.Data.(WALReplicationMessage).Data)

		if err != nil {
			log.Println("Failed to decode WAL data: ", err)
			return err
		}

		err = nr.databaseWalSynchronizer.Sync(
			message.Data.(WALReplicationMessage).DatabaseUuid,
			message.Data.(WALReplicationMessage).BranchUuid,
			decompressedData,
			message.Data.(WALReplicationMessage).Offset,
			message.Data.(WALReplicationMessage).Length,
			message.Data.(WALReplicationMessage).Sha256,
			message.Data.(WALReplicationMessage).Timestamp,
		)

		if err != nil {
			log.Println("Failed to sync WAL data: ", err)
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
		"address": Node().Address(),
		"group":   config.Get().NodeType,
	}

	jsonData, err := json.Marshal(data)

	if err != nil {
		return err
	}

	request, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))

	if err != nil {
		log.Println("Failed to join cluster: ", err)
		return err
	}

	encryptedHeader, err := auth.SecretsManager().Encrypt(
		config.Get().Signature,
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
	httpClient := &http.Client{
		Timeout: 3 * time.Second,
	}

	if nr.node.primaryAddress == "" {
		return nil
	}

	url := fmt.Sprintf("http://%s/cluster/members/%s", nr.node.primaryAddress, nr.node.Address())

	request, err := http.NewRequest("DELETE", url, nil)

	if err != nil {
		log.Println("Failed to leave cluster: ", err)
		return err
	}

	encryptedHeader, err := auth.SecretsManager().Encrypt(
		config.Get().Signature,
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

	request, err := http.NewRequest("POST", fmt.Sprintf("http://%s/cluster/primary", Node().PrimaryAddress()), data)

	if err != nil {
		log.Println("Failed to send message: ", err)
		return NodeMessage{}, err
	}

	encryptedHeader, err := auth.SecretsManager().Encrypt(
		config.Get().Signature,
		Node().Address(),
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
	if Node().PrimaryAddress() == "" {
		return nil, errors.New("Primary address is not set")
	}

	data := bytes.NewBuffer(nil)
	encoder := gob.NewEncoder(data)

	err := encoder.Encode(nodeMessage)

	if err != nil {
		log.Println("Failed to encode message: ", err)
		return nil, err
	}

	request, err := http.NewRequest("POST", fmt.Sprintf("http://%s/cluster/primary", Node().PrimaryAddress()), data)

	if err != nil {
		log.Println("Failed to send message: ", err)
		return nil, err
	}

	encryptedHeader, err := auth.SecretsManager().Encrypt(
		config.Get().Signature,
		Node().Address(),
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
// 	primaryAddress := Node().PrimaryAddress()

// 	if primaryAddress == "" || primaryAddress == Node().Address() {
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
