package database

import (
	"errors"
	"log"
	"sync"
	"time"
)

// TODO: Checkpoint idle databases
// TODO: Close idle connections

type ConnectionManagerInstance struct {
	mutext    *sync.RWMutex
	databases map[string]*BranchGroup
}

type BranchGroup struct {
	checkpointedAt time.Time
	branches       map[string][]*ClientConnection
	locks          map[string]*sync.RWMutex
	lockMutex      *sync.RWMutex
}

var StaticConnectionManagerInstance *ConnectionManagerInstance

func NewBranchGroup() *BranchGroup {
	return &BranchGroup{
		branches:  map[string][]*ClientConnection{},
		locks:     map[string]*sync.RWMutex{},
		lockMutex: &sync.RWMutex{},
	}
}

func ConnectionManager() *ConnectionManagerInstance {
	if StaticConnectionManagerInstance == nil {
		StaticConnectionManagerInstance = &ConnectionManagerInstance{
			mutext:    &sync.RWMutex{},
			databases: map[string]*BranchGroup{},
		}
	}

	return StaticConnectionManagerInstance
}

func (c *ConnectionManagerInstance) Checkpoint(branchGroup *BranchGroup, branchUuid string, clientConnection *ClientConnection) {
	// Skip if the checkpoint time is empty
	if branchGroup.checkpointedAt == (time.Time{}) && clientConnection.connection.commitedAt.Before(branchGroup.checkpointedAt) {
		return
	}

	// Skip if the database connection is before the checkpoint time
	if clientConnection.connection.commitedAt.Before(branchGroup.checkpointedAt) {
		return
	}

	// Skip if the last checkpoint was performed in less than a second
	if time.Since(branchGroup.checkpointedAt) <= 1*time.Second {
		return
	}

	branchGroup.lockMutex.RLock()
	lock := branchGroup.locks[branchUuid]
	branchGroup.lockMutex.RUnlock()

	lock.Lock()
	defer lock.Unlock()

	// Checkpoint database
	err := clientConnection.connection.Checkpoint()

	if err != nil {
		log.Println("Error checkpointing database", err)
		return
	}

	branchGroup.checkpointedAt = time.Now()
	log.Println("Checkpointed database", clientConnection.databaseUuid, clientConnection.branchUuid)
}

func (c *ConnectionManagerInstance) Get(databaseUuid string, branchUuid string) (*ClientConnection, error) {
	c.mutext.Lock()

	var (
		databaseExists bool
		branchExists   bool
	)

	databaseExists = c.databases[databaseUuid] != nil
	branchExists = databaseExists && c.databases[databaseUuid].branches[branchUuid] != nil && len(c.databases[databaseUuid].branches[branchUuid]) > 0

	if databaseExists && branchExists {
		connection := c.databases[databaseUuid].branches[branchUuid][0]
		c.databases[databaseUuid].branches[branchUuid] = c.databases[databaseUuid].branches[branchUuid][1:]

		c.mutext.Unlock()
		return connection, nil
	}
	c.mutext.Unlock()

	c.mutext.Lock()
	if c.databases[databaseUuid] == nil {
		c.databases[databaseUuid] = NewBranchGroup()
	}
	c.mutext.Unlock()

	c.mutext.Lock()
	defer c.mutext.Unlock()

	if c.databases[databaseUuid].branches[branchUuid] == nil {
		c.databases[databaseUuid].branches[branchUuid] = []*ClientConnection{}
		c.databases[databaseUuid].locks[branchUuid] = &sync.RWMutex{}
	}

	// Create a new client connection, only one connection can be created at a
	// time to avoid SQL Logic errors on sqlite3_open.
	con := NewClientConnection(databaseUuid, branchUuid)

	if con == nil {
		return nil, errors.New("connection error")
	}

	return con, nil
}

func (c *ConnectionManagerInstance) GetLock(databaseUuid, branchUuid string) *sync.RWMutex {
	c.mutext.RLock()
	branchGroup := c.databases[databaseUuid]
	c.mutext.RUnlock()

	branchGroup.lockMutex.RLock()
	lock := branchGroup.locks[branchUuid]
	branchGroup.lockMutex.RUnlock()

	return lock
}

func (c *ConnectionManagerInstance) Release(databaseUuid string, branchUuid string, clientConnection *ClientConnection) {
	c.mutext.Lock()
	defer c.mutext.Unlock()

	c.Checkpoint(c.databases[databaseUuid], branchUuid, clientConnection)

	c.databases[databaseUuid].branches[branchUuid] = append(c.databases[databaseUuid].branches[branchUuid], clientConnection)

}

func (c *ConnectionManagerInstance) Remove(databaseUuid string, branchUuid string, clientConnection *ClientConnection) {
	c.mutext.Lock()
	defer c.mutext.Unlock()

	c.Checkpoint(c.databases[databaseUuid], branchUuid, clientConnection)

	// Remove the branch from the database
	delete(c.databases[databaseUuid].branches, branchUuid)

	// If there are no more branches, remove the database
	if len(c.databases[databaseUuid].branches[branchUuid]) == 0 {
		delete(c.databases, databaseUuid)
	}

	clientConnection.Close()
}

func (c *ConnectionManagerInstance) Shutdown() {
	c.mutext.Lock()
	defer c.mutext.Unlock()

	// Close all connections
	for _, database := range c.databases {
		for _, branches := range database.branches {
			for _, connection := range branches {
				connection.Close()
			}
		}
	}

	c.databases = map[string]*BranchGroup{}
}
