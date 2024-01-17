package database

import (
	"sync"
)

type ConnectionManagerInstance struct {
	connections       map[string]map[string]chan *ClientConnection
	connectionMutexes map[string]map[string]*sync.RWMutex
	mutex             *sync.Mutex
}

var StaticConnectionManagerInstance *ConnectionManagerInstance

func ConnectionManager() *ConnectionManagerInstance {
	if StaticConnectionManagerInstance == nil {
		StaticConnectionManagerInstance = &ConnectionManagerInstance{
			connections:       map[string]map[string]chan *ClientConnection{},
			connectionMutexes: map[string]map[string]*sync.RWMutex{},
			mutex:             &sync.Mutex{},
		}
	}

	return StaticConnectionManagerInstance
}

func (c *ConnectionManagerInstance) Clear() {
	c.connections = map[string]map[string]chan *ClientConnection{}
}

func (c *ConnectionManagerInstance) Get(databaseUuid string, branchUuid string) (*ClientConnection, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	clientConnections := c.connections[databaseUuid][branchUuid]

	if len(clientConnections) > 0 {
		return <-clientConnections, nil
	}

	database, err := Get(databaseUuid)

	if err != nil {
		return nil, err
	}

	path := database.BranchDatabaseFile(branchUuid)

	if c.connections[databaseUuid] == nil {
		c.connections[databaseUuid] = map[string]chan *ClientConnection{}
	}

	c.connections[databaseUuid][branchUuid] = make(chan *ClientConnection, 10)

	// for i := 0; i < 10; i++ {
	if len(c.connections[databaseUuid][branchUuid]) < 10 {
		c.connections[databaseUuid][branchUuid] <- NewClientConnection(path, databaseUuid, branchUuid)
	}

	return <-c.connections[databaseUuid][branchUuid], nil
}

func (c *ConnectionManagerInstance) GetMutex(databaseUuid string, branchUuid string) *sync.RWMutex {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.connectionMutexes[databaseUuid] == nil {
		c.connectionMutexes[databaseUuid] = map[string]*sync.RWMutex{}
	}

	if c.connectionMutexes[databaseUuid][branchUuid] == nil {
		c.connectionMutexes[databaseUuid][branchUuid] = &sync.RWMutex{}
	}

	return c.connectionMutexes[databaseUuid][branchUuid]
}

func (c *ConnectionManagerInstance) Release(databaseUuid string, branchUuid string, clientConnection *ClientConnection) {
	c.connections[databaseUuid][branchUuid] <- clientConnection.WithAccessKey(nil)
}

func (c *ConnectionManagerInstance) Remove(databaseUuid string, branchUuid string, clientConnection *ClientConnection) {
	clientConnection.Close()

	c.connections[databaseUuid][branchUuid] <- NewClientConnection(clientConnection.Path(), databaseUuid, branchUuid)
}
