package storage

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"litebase/internal/config"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// This buffer pool can be shared between storage connections to reduce memory
// allocations when reading and writing messages.
var storageConnectionBufferPool = sync.Pool{
	New: func() interface{} {
		return bytes.NewBuffer(make([]byte, 4096))
	},
}

// After this amount of time without receiving a message from the storage node,
// the connection will be closed.
const StorageConnectionInactiveTimeout = 5 * time.Second

// Storage connections are used to read and write files on a storage node in the
// distributed file system. The connection is established when the first request is
// sent and closed after a period of inactivity.
type StorageConnection struct {
	Address         string
	cancel          context.CancelFunc
	config          *config.Config
	connected       chan struct{}
	connecting      bool
	context         context.Context
	errorChan       chan error
	httpClient      *http.Client
	inactiveTimeout *time.Timer
	Index           int
	mutex           *sync.Mutex
	open            bool
	reader          *io.PipeReader
	response        chan DistributedFileSystemResponse
	writer          *io.PipeWriter
	writeBuffer     *bufio.Writer
}

// Create a new storage connection instance.
func NewStorageConnection(c *config.Config, index int, address string) *StorageConnection {
	return &StorageConnection{
		Address: address,
		config:  c,
		Index:   index,
		mutex:   &sync.Mutex{},
	}
}

// Close the connection to the storage node.
func (sc *StorageConnection) closeConnection() error {
	if sc.cancel != nil {
		sc.cancel()
	}

	if sc.writer != nil {
		sc.writer.Close()
	}

	if sc.reader != nil {
		sc.reader.Close()
	}

	sc.connecting = false
	sc.open = false

	return nil
}

// Close the connection to the storage node. This method is thread safe.
func (sc *StorageConnection) Close() error {
	sc.mutex.Lock()
	defer sc.mutex.Unlock()

	sc.closeConnection()

	return nil
}

// Connect to the storage node.
func (sc *StorageConnection) connect() error {
	sc.connecting = true
	sc.connected = make(chan struct{})
	sc.errorChan = make(chan error)
	sc.response = make(chan DistributedFileSystemResponse)

	if sc.httpClient == nil {
		sc.createHTTPClient()
	}

	response, err := sc.createAndSendRequest()

	if err != nil {
		log.Println("failed to connect to storage node: ", err)
		sc.handleError(err)
		return err
	}

	if response.StatusCode != 200 {
		sc.handleError(fmt.Errorf("failed to connect to storage node: %s", response.Status))
		return errors.New("failed to connect to storage node")
	}

	go sc.handleResponse(response)

	select {
	case <-storageContext.Done():
		return errors.New("storage context closed")
	case <-sc.connected:
	case err := <-sc.errorChan:
		return err
	}

	return nil
}

// Create the storage connection request and send it to the storage node.
func (sc *StorageConnection) createAndSendRequest() (*http.Response, error) {
	sc.context, sc.cancel = context.WithCancel(storageContext)
	sc.reader, sc.writer = io.Pipe()
	sc.writeBuffer = bufio.NewWriterSize(sc.writer, 1024)

	request, err := http.NewRequestWithContext(sc.context, "POST", fmt.Sprintf("http://%s/storage", sc.Address), sc.reader)

	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	encryptedHeader, err := StorageEncryption.Encrypt(sc.config.Signature, NodeIPAddress)

	if err != nil {
		return nil, fmt.Errorf("failed to encrypt header: %w", err)
	}

	request.Header.Set("X-Lbdb-Node", encryptedHeader)
	request.Header.Set("X-Lbdb-Node-Timestamp", fmt.Sprintf("%d", time.Now().UnixNano()))

	go sc.writeConnectionRequest()

	response, err := sc.httpClient.Do(request)

	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}

	return response, nil
}

// Create the http client for the storage connection.
func (sc *StorageConnection) createHTTPClient() {
	if sc.httpClient != nil {
		sc.httpClient = nil
	}

	sc.httpClient = &http.Client{
		Timeout: 0,
		Transport: &http.Transport{
			DisableKeepAlives: true,
			DialContext: (&net.Dialer{
				Timeout: 3 * time.Second, // Timeout for establishing a connection
			}).DialContext,
			// IdleConnTimeout:   1 * time.Second,
		},
	}
}

// Handle an error that occurred while connecting to the storage node.
func (sc *StorageConnection) handleError(err error) {
	log.Println(err)
	sc.closeConnection()
}

// Handle the response from the storage node.
func (sc *StorageConnection) handleResponse(response *http.Response) {
	defer response.Body.Close()

	sc.inactiveTimeout = time.NewTimer(StorageConnectionInactiveTimeout)

	go sc.read(sc.cancel, response.Body)

readMessages:
	for {
		select {
		case <-sc.inactiveTimeout.C:
			break readMessages
		case <-sc.context.Done():
			log.Println("context done")
			break readMessages
		}
	}

	sc.open = false
	sc.closeConnection()
}

// Check if the storage connection is open.
func (sc *StorageConnection) IsOpen() bool {
	return sc.open
}

// Read messages from the storage node.
func (sc *StorageConnection) read(
	cancel context.CancelFunc,
	reader io.Reader,
) {
	var dfsResponse DistributedFileSystemResponse = DistributedFileSystemResponse{}

	messageLengthBytes := make([]byte, 4)

	scanBuffer := storageConnectionBufferPool.Get().(*bytes.Buffer)
	defer storageConnectionBufferPool.Put(scanBuffer)

	for {
		_, err := reader.Read(messageLengthBytes)

		if err != nil {
			sc.closeConnection()
			sc.errorChan <- err
			return
		}

		messageLength := int(binary.LittleEndian.Uint32(messageLengthBytes))

		scanBuffer.Reset()

		// Read the message in chunks
		bytesRead := 0

		if messageLength > scanBuffer.Cap() {
			scanBuffer.Grow(messageLength)
		}

		scannerBytes := scanBuffer.Bytes()

		for bytesRead < messageLength {
			// Determine the size of the next chunk to read
			chunkSize := messageLength - bytesRead

			// Read the chunk into the chunkBuffer
			n, err := reader.Read(scannerBytes[:chunkSize])

			if err != nil {
				log.Println(err)
				sc.closeConnection()
				sc.errorChan <- err
				return
			}

			// Write the chunk to the scanBuffer
			scanBuffer.Write(scannerBytes[:n])
			bytesRead += int(n)
		}

		if bytesRead < messageLength {
			log.Println("Failed to read the complete message")

			sc.errorChan <- errors.New("failed to read the complete message")
			return
		}

		sc.inactiveTimeout.Reset(StorageConnectionInactiveTimeout)

		dfsResponse = DecodeDistributedFileSystemResponse(dfsResponse, scanBuffer.Next(messageLength))

		if dfsResponse.Command == ConnectionStorageCommand {
			sc.open = true
			sc.connecting = false
			sc.connected <- struct{}{}

			continue
		}

		sc.response <- dfsResponse
	}
}

// Send a request to the storage node.
func (sc *StorageConnection) Send(request DistributedFileSystemRequest) (DistributedFileSystemResponse, error, error) {
	sc.mutex.Lock()
	defer sc.mutex.Unlock()

	if !sc.open && !sc.connecting {
		err := sc.connect()

		if err != nil {
			return DistributedFileSystemResponse{}, err, nil
		}
	}

	if sc.writer == nil {
		return DistributedFileSystemResponse{}, errors.New("storage connection closed"), nil
	}

	message := request.Encode()
	messageLength := len(message)
	messageLengthBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(messageLengthBytes, uint32(messageLength))

	_, err := sc.writeBuffer.Write(messageLengthBytes)

	if err != nil {
		log.Println("failed to encode request: ", err)
		sc.closeConnection()
		return DistributedFileSystemResponse{}, err, nil
	}

	_, err = sc.writeBuffer.Write(message)

	if err != nil {
		log.Println("failed to encode request: ", err)
		sc.closeConnection()
		return DistributedFileSystemResponse{}, err, nil
	}

	err = sc.writeBuffer.Flush()

	if err != nil {
		log.Println("failed to flush request: ", err)
		sc.closeConnection()
		return DistributedFileSystemResponse{}, err, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			return DistributedFileSystemResponse{}, errors.New("timeout"), nil
		case <-sc.context.Done():
			return DistributedFileSystemResponse{}, errors.New("context closed"), nil
		case err := <-sc.errorChan:
			return DistributedFileSystemResponse{}, err, nil
		case response := <-sc.response:
			if response.Error != "" {
				// Return the proper error for io.EOF
				if response.Error == "EOF" {
					return DistributedFileSystemResponse{}, nil, io.EOF
				}

				// Return the proper error for fs.ErrNotExist
				if response.Error == "file does not exist" || strings.Contains(response.Error, "no such file or directory") {
					return DistributedFileSystemResponse{}, nil, fs.ErrNotExist
				}

				// Return the proper error for fs.ErrorClosed
				if response.Error == "file closed" {
					return DistributedFileSystemResponse{}, nil, fs.ErrClosed
				}

				// Return the proper error for fs.ErrInvalid
				if response.Error == "invalid argument" {
					return DistributedFileSystemResponse{}, nil, fs.ErrInvalid
				}

				// Return the proper error for fs.ErrPermission
				if response.Error == "permission denied" {
					return DistributedFileSystemResponse{}, nil, fs.ErrPermission
				}

				return DistributedFileSystemResponse{}, nil, errors.New(response.Error)
			}

			return response, nil, nil
		default:
		}
	}
}

// Write the connection request to the storage node to establish the connection.
func (sc *StorageConnection) writeConnectionRequest() {
	if sc.writer == nil {
		return
	}

	message := DistributedFileSystemRequest{
		Command: ConnectionStorageCommand,
	}.Encode()
	messageLength := len(message)
	messageLengthBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(messageLengthBytes, uint32(messageLength))

	_, err := sc.writer.Write(messageLengthBytes)

	if err != nil {
		sc.handleError(fmt.Errorf("failed to write connection request: %w", err))
		return
	}

	_, err = sc.writer.Write(message)

	if err != nil {
		sc.handleError(fmt.Errorf("failed to write connection request: %w", err))
		return
	}

	err = sc.writeBuffer.Flush()

	if err != nil {
		sc.handleError(fmt.Errorf("failed to flush connection request: %w", err))
	}
}
