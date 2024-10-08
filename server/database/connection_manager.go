package database

import (
	"fmt"
	"litebase/server/node"
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

const DatabaseIdleTimeout = 1 * time.Minute

type ConnectionManagerInstance struct {
	connectionTicker *time.Ticker
	databases        map[string]*DatabaseGroup
	mutex            *sync.RWMutex
	state            int
}

var connectionManagerMutex = &sync.RWMutex{}
var StaticConnectionManagerInstance *ConnectionManagerInstance

func ConnectionManager() *ConnectionManagerInstance {
	if StaticConnectionManagerInstance != nil {
		return StaticConnectionManagerInstance
	}

	connectionManagerMutex.Lock()
	defer connectionManagerMutex.Unlock()

	if StaticConnectionManagerInstance == nil {
		StaticConnectionManagerInstance = &ConnectionManagerInstance{
			mutex:     &sync.RWMutex{},
			databases: map[string]*DatabaseGroup{},
			state:     ConnectionManagerStateRunning,
		}

		// Start the connection ticker
		go func() {
			time.Sleep(1 * time.Second)
			StaticConnectionManagerInstance.connectionTicker = time.NewTicker(1 * time.Second)

			for {
				select {
				case <-node.Node().Context().Done():
					return
				case <-StaticConnectionManagerInstance.connectionTicker.C:
					StaticConnectionManagerInstance.Tick()
				}
			}
		}()
	}

	return StaticConnectionManagerInstance
}

func (c *ConnectionManagerInstance) Checkpoint(databaseGroup *DatabaseGroup, branchId string, clientConnection *ClientConnection) bool {
	// Skip if the committed at time time stamp for the connection is empty
	if clientConnection.connection.committedAt.IsZero() {
		return false
	}

	// Skip if the committed at time stamp of the connection is before the last
	// checkpoint of the database group
	if clientConnection.connection.committedAt.Before(databaseGroup.checkpointedAt) {
		return false
	}

	// Skip if the last checkpoint for the database group was performed less
	// than a second
	if time.Since(databaseGroup.checkpointedAt) <= 1*time.Second {
		return false
	}

	databaseGroup.lockMutex.RLock()
	lock := databaseGroup.locks[branchId]
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
	c.mutex.Lock()
	defer c.mutex.Unlock()

	for _, database := range c.databases {
		for branchId, brancheConnections := range database.branches {
			for _, branchConnection := range brancheConnections {
				if c.Checkpoint(database, branchId, branchConnection.connection) {
					// Only checkpoint once per branch
					break
				}
			}
		}
	}
}

func (c *ConnectionManagerInstance) Drain(databaseId string, branchId string, drained func() error) error {
	c.mutex.Lock()

	databaseGroup, ok := c.databases[databaseId]

	if !ok {
		c.mutex.Unlock()

		return drained()
	}

	// TODO: This is causing a deadlock
	// databaseGroup.lockMutex.Lock()
	// defer databaseGroup.lockMutex.Unlock()

	_, ok = databaseGroup.branches[branchId]

	if !ok {
		c.mutex.Unlock()

		return drained()
	}

	// Close all idle connections
	// closedChannels := make([]chan bool, len(databaseGroup.branches[branchId]))
	// for i := 0; i < len(databaseGroup.branches[branchId]); i++ {
	// 	branchConnection := databaseGroup.branches[branchId][i]

	// 	closedChannels[i] = branchConnection.Unclaimed()

	// 	if <-branchConnection.Unclaimed() {

	// 	}
	// }

	wg := sync.WaitGroup{}

	for i := 0; i < len(databaseGroup.branches[branchId]); i++ {
		wg.Add(1)
		go func(branchConnection *BranchConnection) {
			defer wg.Done()
			timeout := time.After(3 * time.Second)

			for {
				select {
				case <-branchConnection.Unclaimed():
					branchConnection.connection.Close()
					// databaseGroup.branches[branchId] = append(databaseGroup.branches[branchId][:i], databaseGroup.branches[branchId][i+1:]...)
					return
				case <-timeout:
					branchConnection.Close()
					// databaseGroup.branches[branchId] = append(databaseGroup.branches[branchId][:i], databaseGroup.branches[branchId][i+1:]...)
					return
				}
			}
		}(databaseGroup.branches[branchId][i])
	}

	wg.Wait()

	c.mutex.Unlock()

	// // Wait for all connections to close
	// var retries = 0

	// // Wait for all BranchConnection <-Unclaimed() to be true
	// for {
	// 	log.Println("retries", retries)
	// 	if len(databaseGroup.branches[branchId]) == 0 || retries > 100 {
	// 		break
	// 	}

	// 	time.Sleep(10 * time.Millisecond)

	// 	c.mutex.Lock()
	// 	for i := 0; i < len(databaseGroup.branches[branchId]); {
	// 		branchConnection := databaseGroup.branches[branchId][i]

	// 		if !branchConnection.Claimed() {
	// 			branchConnection.connection.Close()
	// 			// Remove the closed connection from the slice
	// 			databaseGroup.branches[branchId] = append(databaseGroup.branches[branchId][:i], databaseGroup.branches[branchId][i+1:]...)
	// 		} else {
	// 			i++
	// 		}
	// 	}
	// 	c.mutex.Unlock()

	// 	retries++
	// }

	// c.mutex.Lock()
	// defer c.mutex.Unlock()

	// // Force close all connections
	// for _, branchConnection := range databaseGroup.branches[branchId] {
	// 	branchConnection.connection.Close()
	// }

	// Remove the branch from the database group
	databaseGroup.lockMutex.Lock()
	defer databaseGroup.lockMutex.Unlock()

	return drained()
}

// func (c *ConnectionManagerInstance) ensureBranchGroupExists(databaseId string) {
// 	databaseGroup, ok := c.databases[databaseId]

// 	if !ok {
// 		c.databases[databaseId] = NewDatabaseGroup()
// 		c.databases[databaseId].lockMutex.Lock()
// 		defer c.databases[databaseId].lockMutex.Unlock()
// 	}

// 	return databaseGroup
// }

func (c *ConnectionManagerInstance) ensureDatabaseBranchExists(databaseId, branchId string) {
	_, ok := c.databases[databaseId]

	if !ok {
		c.databases[databaseId] = NewDatabaseGroup()
		c.databases[databaseId].lockMutex.Lock()
		defer c.databases[databaseId].lockMutex.Unlock()
	}

	if c.databases[databaseId].branches[branchId] == nil {
		c.databases[databaseId].branches[branchId] = []*BranchConnection{}
		c.databases[databaseId].locks[branchId] = &sync.RWMutex{}
	}
}

func (c *ConnectionManagerInstance) ForceCheckpoint(databaseId string, branchId string) error {
	connection, err := c.Get(databaseId, branchId)

	if err != nil {
		return err
	}

	defer c.Release(databaseId, branchId, connection)

	databaseGroup := c.databases[databaseId]

	if databaseGroup == nil {
		return fmt.Errorf("database group not found")
	}

	databaseGroup.lockMutex.RLock()
	lock := databaseGroup.locks[branchId]
	databaseGroup.lockMutex.RUnlock()

	// Lock the branch to allow the checkpoint to complete
	lock.Lock()

	defer lock.Unlock()

	err = connection.connection.Checkpoint()

	if err != nil {
		return err
	}

	databaseGroup.checkpointedAt = time.Now()

	return nil
}

func (c *ConnectionManagerInstance) Get(databaseId string, branchId string) (*ClientConnection, error) {
	if err := c.StateError(); err != nil {
		return nil, err
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	// if c.databases[databaseId] != nil {
	// 	c.databases[databaseId].lockMutex.Lock()
	// 	defer c.databases[databaseId].lockMutex.Unlock()
	// }

	if c.databases[databaseId] != nil &&
		c.databases[databaseId].branches[branchId] != nil &&
		len(c.databases[databaseId].branches[branchId]) > 0 {
		for _, branchConnection := range c.databases[databaseId].branches[branchId] {
			if !branchConnection.Claimed() {
				branchConnection.Claim()

				return branchConnection.connection, nil
			}
		}
	}

	c.ensureDatabaseBranchExists(databaseId, branchId)

	// Create a new client connection, only one connection can be created at a
	// time to avoid SQL Logic errors on sqlite3_open.
	con, err := NewClientConnection(databaseId, branchId)

	if err != nil {
		return nil, err
	}

	c.databases[databaseId].branches[branchId] = append(c.databases[databaseId].branches[branchId], NewBranchConnection(
		c.databases[databaseId],
		con,
	))

	return con, nil
}

func (c *ConnectionManagerInstance) Release(databaseId string, branchId string, clientConnection *ClientConnection) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.databases[databaseId] == nil {
		return
	}

	for _, branchConnection := range c.databases[databaseId].branches[branchId] {
		if branchConnection.connection.connection.Id() == clientConnection.connection.Id() {
			branchConnection.Unclaim()
			branchConnection.lastUsedAt = time.Now()
			break
		}
	}
}

// Remove a branch connection from the database group. This method is called
// without the mutex lock, so it should be called from within a mutex lock.
func (c *ConnectionManagerInstance) remove(databaseId string, branchId string, clientConnection *ClientConnection) {
	// Remove the branch conenction from the database group branch
	for i, branchConnection := range c.databases[databaseId].branches[branchId] {
		if branchConnection.connection.connection.Id() == clientConnection.connection.Id() {
			c.databases[databaseId].branches[branchId] = append(c.databases[databaseId].branches[branchId][:i], c.databases[databaseId].branches[branchId][i+1:]...)
			break
		}
	}

	// If there are no more branches, remove the database
	if len(c.databases[databaseId].branches[branchId]) == 0 {
		delete(c.databases[databaseId].branches, branchId)
		Resources(databaseId, branchId).Remove()
	}

	clientConnection.Close()
}

func (c *ConnectionManagerInstance) Remove(databaseId string, branchId string, clientConnection *ClientConnection) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.remove(databaseId, branchId, clientConnection)
}

func (c *ConnectionManagerInstance) RemoveIdleConnections() {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	for databaseId, database := range c.databases {
		var activeBranches = len(database.branches)

		for branchId, branchConnections := range database.branches {
			var activeConnections = 0

			for i, branchConnection := range branchConnections {
				// Close the connection if it is not in use and has been idle
				// for more than a minute. We need to also avoid removing
				// connections that require a checkpoint. Not doing so can lead
				// to database corruption.
				if !branchConnection.RequiresCheckpoint() && !branchConnection.Claimed() && time.Since(branchConnection.lastUsedAt) > DatabaseIdleTimeout {
					database.branches[branchId] = append(branchConnections[:i], branchConnections[i+1:]...)
					branchConnection.connection.Close()
				} else if branchConnection.RequiresCheckpoint() {
					activeConnections++
				} else {
					activeConnections++
				}
			}

			// if the database branch has no more branch connections, remove the database branch
			if activeConnections == 0 {
				delete(database.branches, branchId)
				Resources(databaseId, branchId).Remove()
			}

			if len(database.branches) == 0 {
				activeBranches--
			}
		}

		// if the database has no more branches, remove the database
		if activeBranches == 0 {
			delete(c.databases, databaseId)
		}
	}
}

func (c *ConnectionManagerInstance) Shutdown() {
	// Drain all connections
	for databaseId, database := range c.databases {
		for branchId := range database.branches {
			c.Drain(databaseId, branchId, func() error {
				return nil
			})
		}
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()
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

func (c *ConnectionManagerInstance) UpdateWal(
	databaseId, branchId string,
	fileSha256 [32]byte,
	timestamp int64,
) error {
	c.mutex.Lock()
	c.ensureDatabaseBranchExists(databaseId, branchId)
	c.mutex.Unlock()

	return nil
}

func (c *ConnectionManagerInstance) Tick() {
	c.CheckpointAll()
	c.RemoveIdleConnections()
}
