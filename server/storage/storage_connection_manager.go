package storage

import (
	"errors"
	"sync"
)

/*
This manager is a singleton that manages the connections to the storage nodes.
*/
type StorageConnectionManager struct {
	connections map[int]*StorageConnection
	mutex       *sync.Mutex
}

/*
Get the singleton instance of the storage connection manager.
*/
var storageConnectionManagerInstance *StorageConnectionManager

var ErrNoStorageNodesAvailable = errors.New("no storage nodes available")

/*
Helper to get the singleton instance of the storage connection manager or create.
*/
func SCM() *StorageConnectionManager {
	if storageConnectionManagerInstance == nil {
		storageConnectionManagerInstance = &StorageConnectionManager{
			connections: make(map[int]*StorageConnection),
			mutex:       &sync.Mutex{},
		}
	}

	return storageConnectionManagerInstance
}

/*
Close all of the connections to the storage nodes.
*/
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

/*
Get the connection to the storage node that should be used for the given key.
*/
func (s *StorageConnectionManager) GetConnection(key string) (*StorageConnection, error) {

	index, address, err := StorageDiscovery.GetStorageNode(key)

	if err != nil {
		return nil, err
	}

	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.connections[index] != nil && !s.connections[index].IsOpen() {
		s.removeConnection(index)
		s.connections[index] = NewStorageConnection(index, "http://"+address+"/storage")
	}

	if s.connections[index] == nil {
		s.connections[index] = NewStorageConnection(index, "http://"+address+"/storage")
	}

	return s.connections[index], nil
}

/*
Remove the connection from the manager.
*/
func (s *StorageConnectionManager) removeConnection(index int) {
	if s.connections[index] == nil {
		return
	}

	s.connections[index].Close()

	delete(s.connections, index)
}

/*
Send a request through the storage manager to the appropriate storage node.
*/
func (s *StorageConnectionManager) Send(request DistributedFileSystemRequest) (DistributedFileSystemResponse, error) {
	connection, err := s.GetConnection(request.Path)

	if err != nil {
		return DistributedFileSystemResponse{}, err
	}

	response, err := connection.Send(request)

	if err != nil {
		s.removeConnection(connection.Index)

		return DistributedFileSystemResponse{}, err
	}

	return response, nil
}
