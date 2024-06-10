package database

import (
	"fmt"
	"log"
	"sync"
	"time"
)

const (
	ConnectionManagerStateRunning = iota
	ConnectionManagerStateDraining
	ConnectionManagerStateShutdown
)

const (
	ErrorConnectionManagerShutdown = "new database connections cannot be created after shutdown"
	ErrorConnectionManagerDraining = "new database connections cannot be created while shutting down"
)

type ConnectionManagerInstance struct {
	connectionTicker *time.Ticker
	databases        map[string]*DatabaseGroup
	mutext           *sync.RWMutex
	state            int
}

type BranchGroupStatus int

type DatabaseGroup struct {
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

var connectionManagerMutex = &sync.RWMutex{}
var StaticConnectionManagerInstance *ConnectionManagerInstance

func NewDatabaseGroup() *DatabaseGroup {
	return &DatabaseGroup{
		branches:  map[string][]*BranchConnection{},
		locks:     map[string]*sync.RWMutex{},
		lockMutex: &sync.RWMutex{},
	}
}

func ConnectionManager() *ConnectionManagerInstance {
	if StaticConnectionManagerInstance != nil {
		return StaticConnectionManagerInstance
	}

	connectionManagerMutex.Lock()
	defer connectionManagerMutex.Unlock()

	if StaticConnectionManagerInstance == nil {
		StaticConnectionManagerInstance = &ConnectionManagerInstance{
			mutext:    &sync.RWMutex{},
			databases: map[string]*DatabaseGroup{},
			state:     ConnectionManagerStateRunning,
		}

		// Start the connection ticker
		go func() {
			time.Sleep(1 * time.Second)
			StaticConnectionManagerInstance.connectionTicker = time.NewTicker(1 * time.Second)

			for range StaticConnectionManagerInstance.connectionTicker.C {
				StaticConnectionManagerInstance.Tick()
			}

			log.Println("Checkpoint timer stopped")
		}()
	}

	return StaticConnectionManagerInstance
}

func (c *ConnectionManagerInstance) Checkpoint(databaseGroup *DatabaseGroup, branchUuid string, clientConnection *ClientConnection) bool {
	// Skip if the checkpoint time is empty
	if clientConnection.connection.commitedAt.IsZero() || clientConnection.connection.commitedAt.Before(databaseGroup.checkpointedAt) {
		return false
	}

	// Skip if the database connection is before the checkpoint time
	if clientConnection.connection.commitedAt.IsZero() || clientConnection.connection.commitedAt.Before(databaseGroup.checkpointedAt) {
		return false
	}

	// Skip if the last checkpoint was performed in less than a second
	if time.Since(databaseGroup.checkpointedAt) <= 1*time.Second {
		return false
	}

	databaseGroup.lockMutex.RLock()
	lock := databaseGroup.locks[branchUuid]
	databaseGroup.lockMutex.RUnlock()

	lock.Lock()
	defer lock.Unlock()

	// Attempt to checkpoint the database. In cases where there are multiple
	// connections attempting to write to the database, the checkpoint will
	// fail and return SQLITE_BUSY. This is expected and we will just try
	// again with another connection. If the other connection is also busy,
	// we will just skip the checkpoint and try again later.
	err := clientConnection.connection.Checkpoint()

	if err != nil {
		log.Println("Error checkpointing database", err)
		return false
	}

	databaseGroup.checkpointedAt = time.Now()

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

func (c *ConnectionManagerInstance) Drain(databaseUuid string, branchUuid string, drained func() error) error {
	c.mutext.Lock()

	databaseGroup, ok := c.databases[databaseUuid]

	if !ok {
		c.mutext.Unlock()

		return drained()
	}

	databaseGroup.lockMutex.Lock()
	defer databaseGroup.lockMutex.Unlock()

	_, ok = databaseGroup.branches[branchUuid]

	if !ok {
		c.mutext.Unlock()

		return drained()
	}

	// Close all idle connections
	for i := 0; i < len(databaseGroup.branches[branchUuid]); {
		branchConnection := databaseGroup.branches[branchUuid][i]

		if !branchConnection.inUse {
			branchConnection.connection.Close()
			// Remove the closed connection from the slice
			databaseGroup.branches[branchUuid] = append(databaseGroup.branches[branchUuid][:i], databaseGroup.branches[branchUuid][i+1:]...)
		} else {
			i++
		}
	}

	c.mutext.Unlock()

	// Wait for all connections to close
	var retries = 0

	for {
		if len(databaseGroup.branches[branchUuid]) == 0 || retries > 10 {
			break
		}

		time.Sleep(100 * time.Millisecond)

		c.mutext.Lock()
		for i := 0; i < len(databaseGroup.branches[branchUuid]); {
			branchConnection := databaseGroup.branches[branchUuid][i]

			if !branchConnection.inUse {
				branchConnection.connection.Close()
				// Remove the closed connection from the slice
				databaseGroup.branches[branchUuid] = append(databaseGroup.branches[branchUuid][:i], databaseGroup.branches[branchUuid][i+1:]...)
			} else {
				i++
			}
		}
		c.mutext.Unlock()

		retries++
	}

	c.mutext.Lock()
	defer c.mutext.Unlock()

	// Force close all connections
	for _, branchConnection := range databaseGroup.branches[branchUuid] {
		branchConnection.connection.ForceClose()
	}

	// Remove the branch from the database group
	delete(databaseGroup.branches, branchUuid)

	return drained()
}

func (c *ConnectionManagerInstance) Get(databaseUuid string, branchUuid string) (*ClientConnection, error) {
	if err := c.StateError(); err != nil {
		return nil, err
	}

	c.mutext.Lock()
	defer c.mutext.Unlock()

	if c.databases[databaseUuid] != nil {
		c.databases[databaseUuid].lockMutex.Lock()
		defer c.databases[databaseUuid].lockMutex.Unlock()
	}

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
		c.databases[databaseUuid] = NewDatabaseGroup()
		c.databases[databaseUuid].lockMutex.Lock()
		defer c.databases[databaseUuid].lockMutex.Unlock()
	}

	if c.databases[databaseUuid].branches[branchUuid] == nil {
		c.databases[databaseUuid].branches[branchUuid] = []*BranchConnection{}
		c.databases[databaseUuid].locks[branchUuid] = &sync.RWMutex{}
	}

	// Create a new client connection, only one connection can be created at a
	// time to avoid SQL Logic errors on sqlite3_open.
	con, err := NewClientConnection(databaseUuid, branchUuid)

	if err != nil {
		return nil, err
	}

	c.databases[databaseUuid].branches[branchUuid] = append(c.databases[databaseUuid].branches[branchUuid], &BranchConnection{
		connection: con,
		inUse:      true,
		lastUsedAt: time.Now(),
	})

	return con, nil
}

func (c *ConnectionManagerInstance) Release(databaseUuid string, branchUuid string, clientConnection *ClientConnection) {
	c.mutext.Lock()
	defer c.mutext.Unlock()

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

	// Remove the branch conenction from the database group branch
	for i, branchConnection := range c.databases[databaseUuid].branches[branchUuid] {
		if branchConnection.connection.connection.Id() == clientConnection.connection.Id() {
			c.databases[databaseUuid].branches[branchUuid] = append(c.databases[databaseUuid].branches[branchUuid][:i], c.databases[databaseUuid].branches[branchUuid][i+1:]...)
			break
		}
	}

	// If there are no more branches, remove the database
	if len(c.databases[databaseUuid].branches[branchUuid]) == 0 {
		delete(c.databases[databaseUuid].branches, branchUuid)
		DatabaseResources().Remove(databaseUuid, branchUuid)
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
				// Close the connection if it is not in use and has been idle for more than a minute
				if !branchConnection.inUse && time.Since(branchConnection.lastUsedAt) > 1*time.Minute {
					database.branches[branchUuid] = append(branchConnections[:i], branchConnections[i+1:]...)
					branchConnection.connection.Close()
				} else {
					activeConnections++
				}
			}

			// if the database branch has no more branch connections, remove the database branch
			if activeConnections == 0 {
				delete(database.branches, branchUuid)
				DatabaseResources().Remove(databaseUuid, branchUuid)
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

	// Drain all connections
	for databaseUuid, database := range c.databases {
		for branchUuid := range database.branches {
			c.Drain(databaseUuid, branchUuid, func() error {
				return nil
			})
		}
	}

	c.mutext.Lock()
	defer c.mutext.Unlock()

	// Stop connection ticker
	if c.connectionTicker != nil {
		c.connectionTicker.Stop()
	}

	c.databases = map[string]*DatabaseGroup{}
}

func (c *ConnectionManagerInstance) StateError() error {
	switch c.state {
	case ConnectionManagerStateShutdown:
		return fmt.Errorf(ErrorConnectionManagerShutdown)
	case ConnectionManagerStateDraining:
		return fmt.Errorf(ErrorConnectionManagerDraining)
	default:
		return nil
	}
}

func (c *ConnectionManagerInstance) Tick() {
	c.CheckpointAll()
	c.RemoveIdleConnections()
}
