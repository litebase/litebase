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
	cconnectionTicker *time.Ticker
	databases         map[string]*BranchGroup
	mutext            *sync.RWMutex
}

type BranchGroup struct {
	checkpointedAt time.Time
	branches       map[string][]*BranchConnection
	locks          map[string]*sync.RWMutex
	lockMutex      *sync.RWMutex
}

type BranchConnection struct {
	connection *ClientConnection
	lastUsedAt time.Time
	inUse      bool
}

var StaticConnectionManagerInstance *ConnectionManagerInstance

func NewBranchGroup() *BranchGroup {
	return &BranchGroup{
		branches:  map[string][]*BranchConnection{},
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

		// Start the connection ticker
		go func() {
			time.Sleep(1 * time.Second)
			StaticConnectionManagerInstance.cconnectionTicker = time.NewTicker(1 * time.Second)

			for range StaticConnectionManagerInstance.cconnectionTicker.C {
				StaticConnectionManagerInstance.Tick()
			}

			log.Println("Checkpoint timer stopped")
		}()
	}

	return StaticConnectionManagerInstance
}

func (c *ConnectionManagerInstance) Checkpoint(branchGroup *BranchGroup, branchUuid string, clientConnection *ClientConnection) bool {
	// Skip if the checkpoint time is empty
	if branchGroup.checkpointedAt == (time.Time{}) && clientConnection.connection.commitedAt.Before(branchGroup.checkpointedAt) {
		return false
	}

	// Skip if the database connection is before the checkpoint time
	if clientConnection.connection.commitedAt.Before(branchGroup.checkpointedAt) {
		return false
	}

	// Skip if the last checkpoint was performed in less than a second
	if time.Since(branchGroup.checkpointedAt) <= 1*time.Second {
		return false
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
		return false
	}

	branchGroup.checkpointedAt = time.Now()

	return true
}

func (c *ConnectionManagerInstance) CheckpointAll() {
	c.mutext.Lock()
	defer c.mutext.Unlock()

	for _, database := range c.databases {
		for branchUuid, brancheConnections := range database.branches {
			for _, branchConnection := range brancheConnections {
				if c.Checkpoint(database, branchUuid, branchConnection.connection) {
					// Only checkpoint once per branch
					break
				}
			}
		}
	}
}

func (c *ConnectionManagerInstance) Get(databaseUuid string, branchUuid string) (*ClientConnection, error) {
	c.mutext.Lock()
	defer c.mutext.Unlock()

	if c.databases[databaseUuid] != nil &&
		c.databases[databaseUuid].branches[branchUuid] != nil &&
		len(c.databases[databaseUuid].branches[branchUuid]) > 0 {
		for _, branchConnection := range c.databases[databaseUuid].branches[branchUuid] {
			if !branchConnection.inUse {
				branchConnection.inUse = true
				return branchConnection.connection, nil
			}
		}
	}

	if c.databases[databaseUuid] == nil {
		c.databases[databaseUuid] = NewBranchGroup()
	}

	if c.databases[databaseUuid].branches[branchUuid] == nil {
		c.databases[databaseUuid].branches[branchUuid] = []*BranchConnection{}
		c.databases[databaseUuid].locks[branchUuid] = &sync.RWMutex{}
	}

	// Create a new client connection, only one connection can be created at a
	// time to avoid SQL Logic errors on sqlite3_open.
	con := NewClientConnection(databaseUuid, branchUuid)

	if con == nil {
		return nil, errors.New("connection error")
	}

	c.databases[databaseUuid].branches[branchUuid] = append(c.databases[databaseUuid].branches[branchUuid], &BranchConnection{
		connection: con,
		inUse:      true,
		lastUsedAt: time.Now(),
	})

	return con, nil
}

func (c *ConnectionManagerInstance) GetLock(databaseUuid, branchUuid string) *sync.RWMutex {
	c.mutext.RLock()
	defer c.mutext.RUnlock()
	branchGroup, ok := c.databases[databaseUuid]

	if !ok {
		panic("database not found")
	}

	branchGroup.lockMutex.RLock()
	lock := branchGroup.locks[branchUuid]
	branchGroup.lockMutex.RUnlock()

	return lock
}

func (c *ConnectionManagerInstance) Release(databaseUuid string, branchUuid string, clientConnection *ClientConnection) {
	c.mutext.Lock()
	defer c.mutext.Unlock()

	c.Checkpoint(c.databases[databaseUuid], branchUuid, clientConnection)

	for i, branchConnection := range c.databases[databaseUuid].branches[branchUuid] {
		if branchConnection.connection.connection.Id() == clientConnection.connection.Id() {
			c.databases[databaseUuid].branches[branchUuid][i].inUse = false
			c.databases[databaseUuid].branches[branchUuid][i].lastUsedAt = time.Now()
			break
		}
	}
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

func (c *ConnectionManagerInstance) RemoveIdleConnections() {
	c.mutext.Lock()
	defer c.mutext.Unlock()

	for databaseUuid, database := range c.databases {
		var activeBranches = len(database.branches)

		for branchUuid, branchConnections := range database.branches {
			var activeConnections = 0

			for i, branchConnection := range branchConnections {
				if !branchConnection.inUse && time.Since(branchConnection.lastUsedAt) > 1*time.Second {
					database.branches[branchUuid] = append(branchConnections[:i], branchConnections[i+1:]...)
					branchConnection.connection.Close()
				} else {
					activeConnections++
				}
			}

			// if the database branch has no more branch connections, remove the database branch
			if activeConnections == 0 {
				delete(database.branches, branchUuid)
			}

			if len(database.branches) == 0 {
				activeBranches--
			}
		}

		// if the database has no more branches, remove the database
		if activeBranches == 0 {
			delete(c.databases, databaseUuid)
		}
	}
}

func (c *ConnectionManagerInstance) Shutdown() {
	c.mutext.Lock()
	defer c.mutext.Unlock()

	// Stop connection ticker
	if c.cconnectionTicker != nil {
		c.cconnectionTicker.Stop()
	}

	// Close all connections
	for _, database := range c.databases {
		for _, branchConnections := range database.branches {
			for _, branchConnection := range branchConnections {
				branchConnection.connection.Close()
			}
		}
	}

	c.databases = map[string]*BranchGroup{}
}

func (c *ConnectionManagerInstance) Tick() {
	c.CheckpointAll()
	c.RemoveIdleConnections()
}
