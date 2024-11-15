package cluster

import (
	"bufio"
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"litebase/server/cluster/messages"
	"log"
	"net"
	"net/http"
	"sync"
	"time"
)

// After this amount of time without receiving a message from the node,
// the connection will be closed.
const NodeConnectionInactiveTimeout = 5 * time.Second

type NodeConnection struct {
	Address         string
	cancel          context.CancelFunc
	connected       chan struct{}
	connecting      bool
	context         context.Context
	errorChan       chan error
	httpClient      *http.Client
	inactiveTimeout *time.Timer
	mutex           *sync.Mutex
	node            *Node
	open            bool
	reader          *io.PipeReader
	response        chan interface{}
	writer          *io.PipeWriter
	writeBuffer     *bufio.Writer
}

func NewNodeConnection(node *Node, address string) *NodeConnection {
	return &NodeConnection{
		Address:         address,
		connected:       make(chan struct{}),
		connecting:      false,
		errorChan:       make(chan error),
		httpClient:      &http.Client{},
		inactiveTimeout: time.NewTimer(NodeConnectionInactiveTimeout),
		mutex:           &sync.Mutex{},
		node:            node,
		open:            false,
		reader:          nil,
		response:        make(chan interface{}),
		writer:          nil,
		writeBuffer:     nil,
	}
}

// Close the connection to another node.
func (nc *NodeConnection) Close() error {
	nc.mutex.Lock()
	defer nc.mutex.Unlock()

	nc.closeConnection()

	return nil
}

// Close the connection to the node. This is done without a mutex lock so it can be
// called from within a mutex lock.
func (nc *NodeConnection) closeConnection() {
	nc.open = false
	nc.connecting = false

	if nc.cancel != nil {
		nc.cancel()
	}

	if nc.reader != nil {
		nc.reader.Close()
		nc.reader = nil
	}

	if nc.writer != nil {
		nc.writer.Close()
		nc.writer = nil
	}
}

// Connect to the node.
func (nc *NodeConnection) connect() error {
	nc.connecting = true

	response, err := nc.createAndSendRequest()

	if err != nil {
		nc.handleError(err)
		return err
	}

	go nc.handleResponse(response)

	select {
	case <-nc.node.Context().Done():
		return errors.New("node context closed")
	case <-nc.context.Done():
		return errors.New("connection context closed")
	case <-nc.connected:
	case err := <-nc.errorChan:
		return err
	}

	return nil
}

// Create the node connection request and send it to the node.
func (nc *NodeConnection) createAndSendRequest() (*http.Response, error) {
	nc.context, nc.cancel = context.WithCancel(context.Background())
	nc.reader, nc.writer = io.Pipe()
	nc.writeBuffer = bufio.NewWriterSize(nc.writer, 1024)

	request, err := http.NewRequestWithContext(
		nc.context,
		"POST",
		fmt.Sprintf("http://%s/cluster/connection", nc.Address),
		nc.reader,
	)

	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	encryptedHeader, err := nc.node.cluster.Auth.SecretsManager.Encrypt(
		nc.node.cluster.Config.Signature,
		[]byte(nc.node.Address()),
	)

	if err != nil {
		return nil, fmt.Errorf("failed to encrypt header: %w", err)
	}

	request.Header.Set("X-Lbdb-Node", string(encryptedHeader))
	request.Header.Set("X-Lbdb-Node-Timestamp", fmt.Sprintf("%d", time.Now().UnixNano()))

	nc.createHTTPClient()

	go nc.writeConnectionRequest()

	response, err := nc.httpClient.Do(request)

	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}

	return response, nil
}

// Create the http client for the node connection.
func (nc *NodeConnection) createHTTPClient() {
	nc.httpClient = &http.Client{
		Timeout: 0,
		Transport: &http.Transport{
			DisableKeepAlives: true,
			DialContext: (&net.Dialer{
				Timeout: 1 * time.Second, // Timeout for establishing a connection
			}).DialContext,
		},
	}
}

// Handle an error that occurred while connecting to the node.
func (nc *NodeConnection) handleError(err error) {
	if err != nil {
		if err != io.ErrUnexpectedEOF {
			// log.Println(err)
		}
	}

	nc.closeConnection()
}

// Handle the response from the node.
func (nc *NodeConnection) handleResponse(response *http.Response) {
	defer response.Body.Close()

	if response.StatusCode != 200 {
		nc.handleError(fmt.Errorf("failed to connect to node: %s", response.Status))
		return
	}

	go nc.read(response.Body)

	nc.inactiveTimeout = time.NewTimer(NodeConnectionInactiveTimeout)

readMessages:
	for {
		select {
		case <-nc.inactiveTimeout.C:
			break readMessages
		case <-nc.node.Context().Done():
			break readMessages
		case <-nc.context.Done():
			break readMessages
		}
	}

	nc.open = false
	nc.closeConnection()
}

func (nc *NodeConnection) read(reader io.Reader) {
	for {
		select {
		case <-nc.context.Done():
			return
		default:
			decoder := gob.NewDecoder(reader)

			var response messages.NodeMessage

			err := decoder.Decode(&response)

			if err != nil {
				nc.handleError(err)
				return
			}

			nc.inactiveTimeout.Reset(NodeConnectionInactiveTimeout)

			switch message := response.Data.(type) {
			case messages.ErrorMessage:
				nc.errorChan <- errors.New(message.Message)
				return
			case messages.NodeConnectionMessage:
				nc.open = true
				nc.connecting = false
				nc.connected <- struct{}{}
				continue
			}

			nc.response <- response
		}
	}
}

// Send a request to the node.
func (nc *NodeConnection) Send(message messages.NodeMessage) (interface{}, error) {
	nc.mutex.Lock()
	defer nc.mutex.Unlock()

	if !nc.open && !nc.connecting {
		err := nc.connect()

		if err != nil {
			return nil, err
		}
	}

	if nc.writer == nil {
		return nil, errors.New("node connection closed")
	}
	log.Println("sending message")
	encoder := gob.NewEncoder(nc.writer)
	err := encoder.Encode(&message)

	if err != nil {
		log.Println("failed to encode message: ", err)
		nc.closeConnection()
		return nil, err
	}

	err = nc.writeBuffer.Flush()

	if err != nil {
		log.Println("failed to flush request: ", err)
		nc.closeConnection()
		return nil, err
	}

	ctx, cancel := context.WithTimeout(nc.context, 3*time.Second)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			return nil, errors.New("message send timeout")
		case <-nc.node.Context().Done():
			return nil, errors.New("context closed")
		case <-nc.context.Done():
			return nil, errors.New("context closed")
		case err := <-nc.errorChan:
			return nil, err
		case response := <-nc.response:
			return response, nil
		default:
		}
	}
}

// Write the connection request to the node to establish the connection.
func (nc *NodeConnection) writeConnectionRequest() {
	if nc.writer == nil {
		return
	}

	encoder := gob.NewEncoder(nc.writer)

	err := encoder.Encode(messages.NodeMessage{
		Data: messages.NodeConnectionMessage{
			Address: nc.node.Address(),
		},
	})

	if err != nil {
		nc.handleError(fmt.Errorf("failed to write connection request: %w", err))
		return
	}

	err = nc.writeBuffer.Flush()

	if err != nil {
		nc.handleError(fmt.Errorf("failed to flush connection request: %w", err))
	}
}
