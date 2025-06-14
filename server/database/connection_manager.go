package database

import (
	"errors"
	"fmt"
	"log"
	"slices"
	"sync"
	"time"

	"github.com/litebase/litebase/server/cluster"
)

const (
	ConnectionManagerStateRunning = iota
	ConnectionManagerStateDraining
	ConnectionManagerStateShutdown
)

var (
	ErrorConnectionManagerShutdown = errors.New("new database connections cannot be created after shutdown")
	ErrorConnectionManagerDraining = errors.New("new Xdatabase connections cannot be created while shutting down")
)

const DatabaseIdleTimeout = 1 * time.Minute
const DatabaseCheckpointThreshold = 1 * time.Second

type ConnectionManager struct {
	checkpointing    bool
	cluster          *cluster.Cluster
	connectionTicker *time.Ticker
	databaseManager  *DatabaseManager
	databases        map[string]*DatabaseGroup
	mutex            *sync.RWMutex
	state            int
}

func (c *ConnectionManager) Checkpoint(databaseGroup *DatabaseGroup, branchId string, clientConnection *ClientConnection) bool {

	// Skip if the last checkpoint for the database group was performed less
	// than the checkpoint threshold.
	if time.Since(databaseGroup.checkpointedAt) <= DatabaseCheckpointThreshold {
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

	databaseGroup.checkpointedAt = time.Now().UTC()

	return true
}

func (c *ConnectionManager) CheckpointAll() {
	if c.cluster.Node().IsReplica() {
		return
	}

	if c.checkpointing {
		return
	}

	c.checkpointing = true

	defer func() {
		c.checkpointing = false
	}()

	for databaseId, databaseGroup := range c.databases {
		for branchId := range databaseGroup.branches {
			for _, branchConnection := range databaseGroup.branches[branchId] {
				// Skip if the committed at time time stamp for the connection is empty
				if branchConnection.connection.connection.committedAt.IsZero() {
					continue
				}

				// Skip if the committed at time stamp of the connection is before the last
				// checkpoint of the database group
				if branchConnection.connection.connection.committedAt.Before(databaseGroup.checkpointedAt) {
					continue
				}

				connection, err := c.Get(databaseId, branchId)

				if err != nil {
					log.Println("Error getting connection", err)

					continue
				}

				go func(databaseGroup *DatabaseGroup, cc *ClientConnection) {
					c.Checkpoint(databaseGroup, branchId, cc)
					c.Release(cc.connection.databaseId, cc.connection.branchId, cc)
				}(databaseGroup, connection)

				break
			}
		}
	}
}

func (c *ConnectionManager) ClearCaches(databaseId string, branchId string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.databases[databaseId] == nil {
		return
	}

	for _, branchConnection := range c.databases[databaseId].branches[branchId] {
		branchConnection.connection.GetConnection().SqliteConnection().ClearCache()
	}
}

func (c *ConnectionManager) Drain(databaseId string, branchId string, drained func() error) error {
	c.mutex.Lock()

	databaseGroup, ok := c.databases[databaseId]

	if !ok {
		c.mutex.Unlock()

		return drained()
	}

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

	for i := range databaseGroup.branches[branchId] {
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

// func (c *ConnectionManager) ensureBranchGroupExists(databaseId string) {
// 	databaseGroup, ok := c.databases[databaseId]

// 	if !ok {
// 		c.databases[databaseId] = NewDatabaseGroup()
// 		c.databases[databaseId].lockMutex.Lock()
// 		defer c.databases[databaseId].lockMutex.Unlock()
// 	}

// 	return databaseGroup
// }

func (c *ConnectionManager) ensureDatabaseBranchExists(databaseId, branchId string) {
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

func (c *ConnectionManager) ForceCheckpoint(databaseId string, branchId string) error {
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

	databaseGroup.checkpointedAt = time.Now().UTC()

	return nil
}

func (c *ConnectionManager) Get(databaseId string, branchId string) (*ClientConnection, error) {
	if err := c.StateError(); err != nil {
		return nil, err
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

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
	con, err := NewClientConnection(c, databaseId, branchId)

	if err != nil {
		return nil, err
	}

	c.databases[databaseId].branches[branchId] = append(c.databases[databaseId].branches[branchId], NewBranchConnection(
		c.cluster,
		c.databases[databaseId],
		con,
	))

	return con, nil
}

func (c *ConnectionManager) Release(databaseId string, branchId string, clientConnection *ClientConnection) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if clientConnection == nil {
		return
	}

	if c.databases[databaseId] == nil {
		return
	}

	if c.databases[databaseId].branches[branchId] == nil {
		return
	}

	for _, branchConnection := range c.databases[databaseId].branches[branchId] {
		if branchConnection.connection.connection.Id() == clientConnection.connection.Id() {
			branchConnection.Release()
			branchConnection.lastUsedAt = time.Now().UTC()
			break
		}
	}
}

// Remove a branch connection from the database group. This method is called
// without the mutex lock, so it should be called from within a mutex lock.
func (c *ConnectionManager) remove(databaseId string, branchId string, clientConnection *ClientConnection) {
	// Remove the branch connection from the database group branch
	for i, branchConnection := range c.databases[databaseId].branches[branchId] {
		if branchConnection.connection.connection.Id() == clientConnection.connection.Id() {
			c.databases[databaseId].branches[branchId] = slices.Delete(c.databases[databaseId].branches[branchId], i, i+1)
			break
		}
	}

	// If there are no more branches, remove the database
	if len(c.databases[databaseId].branches[branchId]) == 0 {
		delete(c.databases[databaseId].branches, branchId)
		c.databaseManager.Remove(databaseId, branchId)
	}

	clientConnection.Close()
}

func (c *ConnectionManager) Remove(databaseId string, branchId string, clientConnection *ClientConnection) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.remove(databaseId, branchId, clientConnection)
}

func (c *ConnectionManager) RemoveIdleConnections() {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	for databaseId, database := range c.databases {
		var activeBranches = len(database.branches)

		for branchId, branchConnections := range database.branches {
			removeableBranches := []*BranchConnection{}

			for _, branchConnection := range branchConnections {
				// Close the connection if it is not in use and has been idle
				// for more than a minute. We need to also avoid removing
				// connections that require a checkpoint. Not doing so can lead
				// to database corruption.
				if !branchConnection.RequiresCheckpoint() && !branchConnection.Claimed() && time.Since(branchConnection.lastUsedAt) > DatabaseIdleTimeout {
					removeableBranches = append(removeableBranches, branchConnection)
				}
			}

			for _, branchConnection := range removeableBranches {
				c.remove(databaseId, branchId, branchConnection.connection)
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

func (c *ConnectionManager) Shutdown() {
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

func (c *ConnectionManager) StateError() error {
	switch c.state {
	case ConnectionManagerStateShutdown:
		return ErrorConnectionManagerShutdown
	case ConnectionManagerStateDraining:
		return ErrorConnectionManagerDraining
	default:
		return nil
	}
}

func (c *ConnectionManager) Tick() {
	c.CheckpointAll()
	c.RemoveIdleConnections()
}
