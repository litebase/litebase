package storage

import (
	"errors"
	"litebase/internal/config"
	"log"
	"sync"
)

// This manager is a singleton that manages the connections to the storage nodes.
type StorageConnectionManager struct {
	config      *config.Config
	connections map[string]*StorageConnection
	mutex       *sync.Mutex
}

var ErrNoStorageNodesAvailable = errors.New("no storage nodes available")

// Helper to get the singleton instance of the storage connection manager or create.
func NewStorageConnectionManager(config *config.Config) *StorageConnectionManager {
	return &StorageConnectionManager{
		config:      config,
		connections: make(map[string]*StorageConnection),
		mutex:       &sync.Mutex{},
	}
}

// Close all of the connections to the storage nodes.
func (s *StorageConnectionManager) Close() []error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	errors := make([]error, 0)

	for _, connection := range s.connections {
		err := connection.Close()

		if err != nil {
			errors = append(errors, err)
		}
	}

	return errors
}

// Get the connection to the storage node that should be used for the given key.
func (s *StorageConnectionManager) GetConnection(key string) (*StorageConnection, error) {
	index, address, err := StorageDiscovery.GetStorageNode(key)

	if err != nil {
		return nil, err
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.connections[address] != nil && !s.connections[address].IsOpen() {
		s.removeConnection(address)
		s.connections[address] = NewStorageConnection(s.config, index, address)
	}

	if s.connections[address] == nil {
		s.connections[address] = NewStorageConnection(s.config, index, address)
	}

	return s.connections[address], nil
}

// Remove the connection from the manager.
func (s *StorageConnectionManager) removeConnection(address string) {
	if s.connections[address] == nil {
		return
	}

	s.connections[address].Close()

	delete(s.connections, address)
}

// Send a request through the storage manager to the appropriate storage node.
func (s *StorageConnectionManager) Send(request DistributedFileSystemRequest) (DistributedFileSystemResponse, error) {
	connection, err := s.GetConnection(request.Path)

	if err != nil {
		return DistributedFileSystemResponse{}, err
	}

	response, connectionError, fileError := connection.Send(request)

	if connectionError != nil {
		log.Println("Error sending request to storage node:", connectionError, request)
		s.removeConnection(connection.Address)

		return DistributedFileSystemResponse{}, connectionError
	}

	if fileError != nil {
		return DistributedFileSystemResponse{}, fileError
	}

	return response, nil
}
