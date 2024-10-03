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

func (c *ConnectionManagerInstance) Checkpoint(databaseGroup *DatabaseGroup, branchUuid string, clientConnection *ClientConnection) bool {
	// Skip if the checkpoint time is empty
	if clientConnection.connection.committedAt.IsZero() {
		return false
	}

	if clientConnection.connection.committedAt.Before(databaseGroup.checkpointedAt) {
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
	c.mutex.Lock()
	defer c.mutex.Unlock()

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
	c.mutex.Lock()

	databaseGroup, ok := c.databases[databaseUuid]

	if !ok {
		c.mutex.Unlock()

		return drained()
	}

	// TODO: This is causing a deadlock
	// databaseGroup.lockMutex.Lock()
	// defer databaseGroup.lockMutex.Unlock()

	_, ok = databaseGroup.branches[branchUuid]

	if !ok {
		c.mutex.Unlock()

		return drained()
	}

	// Close all idle connections
	// closedChannels := make([]chan bool, len(databaseGroup.branches[branchUuid]))
	// for i := 0; i < len(databaseGroup.branches[branchUuid]); i++ {
	// 	branchConnection := databaseGroup.branches[branchUuid][i]

	// 	closedChannels[i] = branchConnection.Unclaimed()

	// 	if <-branchConnection.Unclaimed() {

	// 	}
	// }

	wg := sync.WaitGroup{}

	for i := 0; i < len(databaseGroup.branches[branchUuid]); i++ {
		wg.Add(1)
		go func(branchConnection *BranchConnection) {
			defer wg.Done()
			timeout := time.After(3 * time.Second)

			for {
				select {
				case <-branchConnection.Unclaimed():
					branchConnection.connection.Close()
					// databaseGroup.branches[branchUuid] = append(databaseGroup.branches[branchUuid][:i], databaseGroup.branches[branchUuid][i+1:]...)
					return
				case <-timeout:
					branchConnection.Close()
					// databaseGroup.branches[branchUuid] = append(databaseGroup.branches[branchUuid][:i], databaseGroup.branches[branchUuid][i+1:]...)
					return
				}
			}
		}(databaseGroup.branches[branchUuid][i])
	}

	wg.Wait()

	c.mutex.Unlock()

	// // Wait for all connections to close
	// var retries = 0

	// // Wait for all BranchConnection <-Unclaimed() to be true
	// for {
	// 	log.Println("retries", retries)
	// 	if len(databaseGroup.branches[branchUuid]) == 0 || retries > 100 {
	// 		break
	// 	}

	// 	time.Sleep(10 * time.Millisecond)

	// 	c.mutex.Lock()
	// 	for i := 0; i < len(databaseGroup.branches[branchUuid]); {
	// 		branchConnection := databaseGroup.branches[branchUuid][i]

	// 		if !branchConnection.Claimed() {
	// 			branchConnection.connection.Close()
	// 			// Remove the closed connection from the slice
	// 			databaseGroup.branches[branchUuid] = append(databaseGroup.branches[branchUuid][:i], databaseGroup.branches[branchUuid][i+1:]...)
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
	// for _, branchConnection := range databaseGroup.branches[branchUuid] {
	// 	branchConnection.connection.Close()
	// }

	// Remove the branch from the database group
	databaseGroup.lockMutex.Lock()
	defer databaseGroup.lockMutex.Unlock()

	return drained()
}

// func (c *ConnectionManagerInstance) ensureBranchGroupExists(databaseUuid string) {
// 	databaseGroup, ok := c.databases[databaseUuid]

// 	if !ok {
// 		c.databases[databaseUuid] = NewDatabaseGroup()
// 		c.databases[databaseUuid].lockMutex.Lock()
// 		defer c.databases[databaseUuid].lockMutex.Unlock()
// 	}

// 	return databaseGroup
// }

func (c *ConnectionManagerInstance) ensureDatabaseBranchExists(databaseUuid, branchUuid string) {
	_, ok := c.databases[databaseUuid]

	if !ok {
		c.databases[databaseUuid] = NewDatabaseGroup()
		c.databases[databaseUuid].lockMutex.Lock()
		defer c.databases[databaseUuid].lockMutex.Unlock()
	}

	if c.databases[databaseUuid].branches[branchUuid] == nil {
		c.databases[databaseUuid].branches[branchUuid] = []*BranchConnection{}
		c.databases[databaseUuid].locks[branchUuid] = &sync.RWMutex{}
	}
}

func (c *ConnectionManagerInstance) ForceCheckpoint(databaseUuid string, branchUuid string) error {
	connection, err := c.Get(databaseUuid, branchUuid)

	if err != nil {
		return err
	}

	defer c.Release(databaseUuid, branchUuid, connection)

	databaseGroup := c.databases[databaseUuid]

	if databaseGroup == nil {
		return fmt.Errorf("database group not found")
	}

	databaseGroup.lockMutex.RLock()
	lock := databaseGroup.locks[branchUuid]
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

func (c *ConnectionManagerInstance) Get(databaseUuid string, branchUuid string) (*ClientConnection, error) {
	if err := c.StateError(); err != nil {
		return nil, err
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	// if c.databases[databaseUuid] != nil {
	// 	c.databases[databaseUuid].lockMutex.Lock()
	// 	defer c.databases[databaseUuid].lockMutex.Unlock()
	// }

	if c.databases[databaseUuid] != nil &&
		c.databases[databaseUuid].branches[branchUuid] != nil &&
		len(c.databases[databaseUuid].branches[branchUuid]) > 0 {
		for _, branchConnection := range c.databases[databaseUuid].branches[branchUuid] {
			if !branchConnection.Claimed() {
				branchConnection.Claim()

				return branchConnection.connection, nil
			}
		}
	}

	c.ensureDatabaseBranchExists(databaseUuid, branchUuid)

	// Create a new client connection, only one connection can be created at a
	// time to avoid SQL Logic errors on sqlite3_open.
	con, err := NewClientConnection(databaseUuid, branchUuid)

	if err != nil {
		return nil, err
	}

	c.databases[databaseUuid].branches[branchUuid] = append(c.databases[databaseUuid].branches[branchUuid], NewBranchConnection(
		c.databases[databaseUuid],
		con,
	))

	return con, nil
}

func (c *ConnectionManagerInstance) Release(databaseUuid string, branchUuid string, clientConnection *ClientConnection) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.databases[databaseUuid] == nil {
		return
	}

	for _, branchConnection := range c.databases[databaseUuid].branches[branchUuid] {
		if branchConnection.connection.connection.Id() == clientConnection.connection.Id() {
			branchConnection.Unclaim()
			branchConnection.lastUsedAt = time.Now()
			break
		}
	}
}

// Remove a branch connection from the database group. This method is called
// without the mutex lock, so it should be called from within a mutex lock.
func (c *ConnectionManagerInstance) remove(databaseUuid string, branchUuid string, clientConnection *ClientConnection) {
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
		Resources(databaseUuid, branchUuid).Remove()
	}

	clientConnection.Close()
}

func (c *ConnectionManagerInstance) Remove(databaseUuid string, branchUuid string, clientConnection *ClientConnection) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.remove(databaseUuid, branchUuid, clientConnection)
}

func (c *ConnectionManagerInstance) RemoveIdleConnections() {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	for databaseUuid, database := range c.databases {
		var activeBranches = len(database.branches)

		for branchUuid, branchConnections := range database.branches {
			var activeConnections = 0

			for i, branchConnection := range branchConnections {
				// Close the connection if it is not in use and has been idle
				// for more than a minute. We need to also avoid removing
				// connections that require a checkpoint. Not doing so can lead
				// to database corruption.
				if !branchConnection.RequiresCheckpoint() && !branchConnection.Claimed() && time.Since(branchConnection.lastUsedAt) > DatabaseIdleTimeout {
					database.branches[branchUuid] = append(branchConnections[:i], branchConnections[i+1:]...)
					branchConnection.connection.Close()
				} else if branchConnection.RequiresCheckpoint() {
					activeConnections++
				} else {
					activeConnections++
				}
			}

			// if the database branch has no more branch connections, remove the database branch
			if activeConnections == 0 {
				delete(database.branches, branchUuid)
				Resources(databaseUuid, branchUuid).Remove()
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
	databaseUuid, branchUuid string,
	fileSha256 [32]byte,
	timestamp int64,
) error {
	c.mutex.Lock()
	c.ensureDatabaseBranchExists(databaseUuid, branchUuid)
	c.mutex.Unlock()

	return nil
}

func (c *ConnectionManagerInstance) Tick() {
	c.CheckpointAll()
	c.RemoveIdleConnections()
}
