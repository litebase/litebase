package node

import (
	"bytes"
	"crypto/sha256"
	"encoding/gob"
	"fmt"
	"io"
	"litebase/internal/config"
	"litebase/server/auth"
	"litebase/server/cluster"
	"litebase/server/storage"
	"log"
	"net/http"
	"os"
	"sync"

	"github.com/klauspost/compress/s2"
)

type NodePrimary struct {
	mutex        *sync.RWMutex
	queryBuilder NodeQueryBuilder
}

func NewNodePrimary(queryBuilder NodeQueryBuilder) *NodePrimary {
	primary := &NodePrimary{
		mutex:        &sync.RWMutex{},
		queryBuilder: queryBuilder,
	}

	return primary
}

func (np *NodePrimary) HandleMessage(message NodeMessage) (NodeMessage, error) {
	var responseMessage NodeMessage

	switch message.Type {
	case "QueryMessage":
		responseMessage = np.handleQueryMessage(message)
	case "WALMessage":
		responseMessage = np.handleWALMessage(message)
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
	query, err := np.queryBuilder.Build(
		message.Data.(QueryMessage).AccessKeyId,
		message.Data.(QueryMessage).DatabaseHash,
		message.Data.(QueryMessage).DatabaseUuid,
		message.Data.(QueryMessage).BranchUuid,
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

func (np *NodePrimary) handleWALMessage(message NodeMessage) NodeMessage {
	path := Node().databaseWalSynchronizer.WalPath(
		message.Data.(WALMessage).DatabaseUuid,
		message.Data.(WALMessage).BranchUuid,
	)

	timestamp, err := Node().databaseWalSynchronizer.WalTimestamp(
		message.Data.(WALMessage).DatabaseUuid,
		message.Data.(WALMessage).BranchUuid,
	)

	if err != nil {
		log.Println("Failed to read WAL: ", err)

		return NodeMessage{
			Error: err.Error(),
			Id:    message.Id,
			Type:  "Error",
		}
	}

	walFile, err := storage.ObjectFS().Open(path)

	if err != nil {
		if os.IsNotExist(err) {
			return NodeMessage{
				Id:          message.Id,
				Type:        "WALMessageResponse",
				EndOfStream: true,
				Data: WALMessageResponse{
					BranchUuid:   message.Data.(WALMessage).BranchUuid,
					ChunkNumber:  1,
					Data:         s2.Encode(nil, []byte{}),
					DatabaseUuid: message.Data.(WALMessage).DatabaseUuid,
					LastChunk:    true,
					Sha256:       [32]byte{},
					Timestamp:    timestamp,
					TotalChunks:  1,
				},
			}
		}

		return NodeMessage{
			Error: err.Error(),
			Id:    message.Id,
			Type:  "Error",
		}
	}

	defer walFile.Close()

	fileInfo, err := walFile.Stat()

	if err != nil {
		log.Println("Failed to stat WAL: ", err)

		return NodeMessage{
			Error: err.Error(),
			Id:    message.Id,
			Type:  "Error",
		}
	}

	size := fileInfo.Size()

	// TODO: Do this by size of WAL frames
	if size <= 1024*1024 {
		data, err := io.ReadAll(walFile)

		if err != nil {
			log.Println("Failed to read WAL: ", err)

			return NodeMessage{
				Error: err.Error(),
				Id:    message.Id,
				Type:  "Error",
			}
		}

		fileSha256 := sha256.Sum256(data)

		return NodeMessage{
			Id:          message.Id,
			Type:        "WALMessageResponse",
			EndOfStream: true,
			Data: WALMessageResponse{
				BranchUuid:   message.Data.(WALMessage).BranchUuid,
				ChunkNumber:  1,
				Data:         s2.Encode(nil, data),
				DatabaseUuid: message.Data.(WALMessage).DatabaseUuid,
				LastChunk:    true,
				Sha256:       fileSha256,
				Timestamp:    timestamp,
				TotalChunks:  1,
			},
		}
	}

	// TODO: Do this by size of WAL frames
	totalChunks := int(size) / (1024 * 1024)
	maxChunkSize := 1024 * 1024
	readBytes := 0
	fileSha256 := sha256.New()

	for {
		// Read the file in chunks
		chunk := make([]byte, maxChunkSize)

		_, err = walFile.Read(chunk)

		if err != nil {
			return NodeMessage{}
		}

		readBytes += len(chunk)
		fileSha256.Write(chunk)
		hashSum := [32]byte{}
		lastChunk := readBytes >= int(size)

		if lastChunk {
			copy(hashSum[:], fileSha256.Sum(nil))
		}

		return NodeMessage{
			Id:          message.Id,
			Type:        "WALMessageResponse",
			EndOfStream: lastChunk,
			Data: WALMessageResponse{
				BranchUuid:   message.Data.(WALMessage).BranchUuid,
				ChunkNumber:  readBytes / maxChunkSize,
				Data:         s2.Encode(nil, chunk),
				DatabaseUuid: message.Data.(WALMessage).DatabaseUuid,
				LastChunk:    lastChunk,
				Sha256:       hashSum,
				Timestamp:    timestamp,
				TotalChunks:  totalChunks,
			},
		}

		// if lastChunk {
		// 	break
		// }
	}

	// return NodeMessage{}
}

func (np *NodePrimary) Heartbeat() error {
	return np.Publish(NodeMessage{
		Id:   "broadcast",
		Type: "HeartbeatMessage",
	})
}

func (np *NodePrimary) Publish(nodeMessage NodeMessage) error {
	np.mutex.RLock()
	defer np.mutex.RUnlock()
	var nodes []*NodeIdentifier

	if config.Get().NodeType == config.NODE_TYPE_QUERY {
		nodes = OtherQueryNodes()
	} else if config.Get().NodeType == config.NODE_TYPE_STORAGE {
		nodes = OtherStorageNodes()
	}

	if len(nodes) == 0 {
		return nil
	}

	data := bytes.NewBuffer(nil)
	encoder := gob.NewEncoder(data)
	err := encoder.Encode(nodeMessage)

	if err != nil {
		log.Println("Failed to encode message: ", err)
		return err
	}

	client := &http.Client{
		Timeout: 0,
	}

	wg := sync.WaitGroup{}

	for _, node := range nodes {
		wg.Add(1)

		go func(node *NodeIdentifier) {
			defer wg.Done()

			request, err := http.NewRequest("POST", fmt.Sprintf("http://%s:%s/cluster/replica", node.Address, node.Port), data)

			if err != nil {
				log.Println("Failed to send message: ", err)
				return
			}

			encryptedHeader, err := auth.SecretsManager().Encrypt(
				config.Get().Signature,
				Node().Address(),
			)

			if err != nil {
				log.Println(err)
				return
			}

			request.Header.Set("X-Lbdb-Node", encryptedHeader)
			request.Header.Set("Content-Type", "application/gob")

			response, err := client.Do(request)

			if err != nil {
				log.Println("Failed to send message: ", err)
				cluster.Get().RemoveMember(node.String())
				return
			}

			if response.StatusCode != 200 {
				log.Println("Failed to send message: ", response.Status)
			}
		}(node)
	}

	wg.Wait()

	return nil
}

func (np *NodePrimary) Start() error {
	return nil
}

func (np *NodePrimary) Stop() {
	// np.mutex.Lock()
	// defer np.mutex.Unlock()

	// for _, connection := range np.connections {
	// 	connection.Close()
	// }
}
