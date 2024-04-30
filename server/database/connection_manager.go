package database

import (
	"errors"
	"sync"
)

type ConnectionManagerInstance struct {
	connections       map[string]map[string][]*ClientConnection
	connectionMutexes map[string]map[string]*sync.RWMutex
	mutex             *sync.RWMutex
}

var StaticConnectionManagerInstance *ConnectionManagerInstance

func ConnectionManager() *ConnectionManagerInstance {
	if StaticConnectionManagerInstance == nil {
		StaticConnectionManagerInstance = &ConnectionManagerInstance{
			connections:       map[string]map[string][]*ClientConnection{},
			connectionMutexes: map[string]map[string]*sync.RWMutex{},
			mutex:             &sync.RWMutex{},
		}
	}

	return StaticConnectionManagerInstance
}

func (c *ConnectionManagerInstance) Clear() {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.connections = map[string]map[string][]*ClientConnection{}
}

func (c *ConnectionManagerInstance) Get(databaseUuid string, branchUuid string) (*ClientConnection, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.connections[databaseUuid][branchUuid] != nil && len(c.connections[databaseUuid][branchUuid]) > 0 {
		connection := c.connections[databaseUuid][branchUuid][0]
		c.connections[databaseUuid][branchUuid] = c.connections[databaseUuid][branchUuid][1:]

		return connection, nil
	}

	if c.connections[databaseUuid] == nil {
		c.connections[databaseUuid] = map[string][]*ClientConnection{}
	}

	if c.connections[databaseUuid][branchUuid] == nil {
		c.connections[databaseUuid][branchUuid] = []*ClientConnection{}
	}

	con := NewClientConnection(databaseUuid, branchUuid)

	if con == nil {
		return nil, errors.New("Connection error")
	}

	return con, nil
}

func (c *ConnectionManagerInstance) Release(databaseUuid string, branchUuid string, clientConnection *ClientConnection) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.connections[databaseUuid] == nil {
		c.connections[databaseUuid] = map[string][]*ClientConnection{}
	}

	if c.connections[databaseUuid][branchUuid] == nil {
		c.connections[databaseUuid][branchUuid] = []*ClientConnection{}
	}

	c.connections[databaseUuid][branchUuid] = append(c.connections[databaseUuid][branchUuid], clientConnection)
}

func (c *ConnectionManagerInstance) Remove(databaseUuid string, branchUuid string, clientConnection *ClientConnection) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	clientConnection.Close()
}
