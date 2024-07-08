package storage

import (
	"context"
	"encoding/gob"
	"errors"
	internalStorage "litebase/internal/storage"
	"net/http"
	"sync"
)

type StorageConnectionManagerInstance struct {
	mutext      *sync.RWMutex
	connections map[string]*StorageConnection
}

var StorageConnectionManagerSingleton *StorageConnectionManagerInstance

func StorageConnectionManager() *StorageConnectionManagerInstance {
	if StorageConnectionManagerSingleton == nil {
		StorageConnectionManagerSingleton = &StorageConnectionManagerInstance{
			mutext:      &sync.RWMutex{},
			connections: map[string]*StorageConnection{},
		}
	}

	return StorageConnectionManagerSingleton
}

func (cm *StorageConnectionManagerInstance) Activate(connection *StorageConnection, w http.ResponseWriter, r *http.Request) error {
	connection.responseWriter = w
	connection.encoder = gob.NewEncoder(w)
	connection.activated <- true
	decoder := gob.NewDecoder(r.Body)

	for {
		var response internalStorage.StorageResponse

		err := decoder.Decode(&response)

		if err != nil {
			connection.Close()
			return err
		}

		connection.responseChannel <- response
	}
}

// Create
func (cm *StorageConnectionManagerInstance) Create(ctx context.Context, connectionUrl string) *StorageConnection {
	connection := NewStorageConnection(ctx, connectionUrl)

	cm.connections[connection.Id] = connection

	return cm.connections[connection.Id]
}

// Get
func (cm *StorageConnectionManagerInstance) Get(connectionId string) (*StorageConnection, error) {
	cm.mutext.Lock()
	defer cm.mutext.Unlock()

	connection, ok := cm.connections[connectionId]

	if !ok {
		return nil, errors.New("no connection found")
	}

	return connection, nil
}

// Remove
func (cm *StorageConnectionManagerInstance) Remove(connectionId string) {
	cm.mutext.Lock()
	defer cm.mutext.Unlock()

	connection := cm.connections[connectionId]

	if connection != nil {
		connection.Close()
	}

	delete(cm.connections, connectionId)
}
