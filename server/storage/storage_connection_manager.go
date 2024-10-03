package storage

import (
	"hash/fnv"
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
func (s *StorageConnectionManager) Close() {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	for _, connection := range s.connections {
		connection.Close()
	}
}

/*
Get the connection to the storage node that should be used for the given key.
*/
func (s *StorageConnectionManager) GetConnection(key string) (*StorageConnection, error) {
	// Get the numerical hash of the key
	h := fnv.New32a()
	h.Write([]byte(key))
	hash := h.Sum32()

	// TODO: Update this to the number of storage nodes that are available
	// Get the index of the storage node that should be used for this key
	index := int(hash) % 1

	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.connections[index] != nil && !s.connections[index].IsOpen() {
		s.connections[index] = NewStorageConnection(index, "http://localhost:8081/storage")
	}

	if s.connections[index] == nil {
		s.connections[index] = NewStorageConnection(index, "http://localhost:8081/storage")
	}

	return s.connections[index], nil
}

/*
Remove the connection from the manager.
*/
func (s *StorageConnectionManager) RemoveConnection(index int) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

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
		return DistributedFileSystemResponse{}, err
	}

	return response, nil
}
