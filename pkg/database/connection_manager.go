package database

import (
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/litebase/litebase/pkg/cluster"
)

const (
	ConnectionManagerStateRunning = iota
	ConnectionManagerStateDraining
	ConnectionManagerStateShutdown
)

var (
	ErrorConnectionManagerShutdown = errors.New("new database connections cannot be created after shutdown")
	ErrorConnectionManagerDraining = errors.New("new database connections cannot be created while shutting down")
	ConnectionDrainingWaitTime     = 3 * time.Second
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

// Checkpoint a database is necessary.
func (c *ConnectionManager) checkpoint(databaseGroup *DatabaseGroup, branchId string, clientConnection *ClientConnection) bool {
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
		slog.Error("Error checkpointing database", "error", err)
		return false
	}

	databaseGroup.checkpointedAt = time.Now().UTC()

	return true
}

// Iterate over all active connections and checkpoint them if necessary.
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
					slog.Error("Error getting connection", "error", err)

					continue
				}

				go func(databaseGroup *DatabaseGroup, cc *ClientConnection) {
					c.checkpoint(databaseGroup, branchId, cc)
					c.Release(cc)
				}(databaseGroup, connection)

				break
			}
		}
	}
}

// Close all connections for a given database.
func (c *ConnectionManager) CloseDatabaseConnections(databaseId string) {
	c.mutex.Lock()

	if c.databases[databaseId] == nil {
		c.mutex.Unlock()
		return
	}

	branches := make([]string, 0, len(c.databases[databaseId].branches))
	for branchId := range c.databases[databaseId].branches {
		branches = append(branches, branchId)
	}

	c.mutex.Unlock()

	for _, branchId := range branches {
		c.CloseDatabaseBranchConnections(databaseId, branchId)
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	delete(c.databases, databaseId)
}

// Close all connections for a given database branch.
func (c *ConnectionManager) CloseDatabaseBranchConnections(databaseId string, branchId string) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if c.databases[databaseId] == nil {
		return
	}

	for _, branchConnection := range c.databases[databaseId].branches[branchId] {
		err := branchConnection.connection.GetConnection().Close()

		if err != nil {
			slog.Error("Error closing connection", "error", err)
		}
	}

	c.databases[databaseId].lockMutex.Lock()
	defer c.databases[databaseId].lockMutex.Unlock()

	delete(c.databases[databaseId].branches, branchId)
}

// Drain all connections for a given database branch. This method will wait for
// all connections to be closed but will allow 3 seconds before returning.
func (c *ConnectionManager) Drain(databaseId string, branchId string, drained func() error) error {
	c.mutex.Lock()

	c.state = ConnectionManagerStateDraining

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

	wg := sync.WaitGroup{}

	for i := range databaseGroup.branches[branchId] {
		wg.Add(1)
		go func(branchConnection *BranchConnection) {
			defer wg.Done()
			timeout := time.After(ConnectionDrainingWaitTime)

			for {
				select {
				case <-branchConnection.Unclaimed():
					branchConnection.connection.Close()
					return
				case <-timeout:
					branchConnection.Close()
					return
				}
			}
		}(databaseGroup.branches[branchId][i])
	}

	wg.Wait()

	c.mutex.Unlock()

	// Remove the branch from the database group
	databaseGroup.lockMutex.Lock()
	defer databaseGroup.lockMutex.Unlock()

	return drained()
}

// Ensure that a database and branch exists in the connection manager.
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

// Force a database to checkpoint by locking the branch and performing a checkpoint.
func (c *ConnectionManager) ForceCheckpoint(databaseId string, branchId string) error {
	connection, err := c.Get(databaseId, branchId)

	if err != nil {
		return err
	}

	defer c.Release(connection)

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

// Get a client connection for a given database and branch. If there are no
// available connections, a new one will be created.
func (c *ConnectionManager) Get(databaseId string, branchId string) (*ClientConnection, error) {
	if err := c.StateError(); err != nil {
		return nil, err
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	database, err := c.databaseManager.Get(databaseId)

	if err != nil {
		slog.Error("Error getting database", "error", err)
		return nil, fmt.Errorf("database '%s' not found", databaseId)
	}

	if !database.HasBranch(branchId) {
		return nil, fmt.Errorf("branch '%s' not found for database '%s'", branchId, databaseId)
	}

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

// Release a client connection back to the connection manager.
func (c *ConnectionManager) Release(clientConnection *ClientConnection) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if clientConnection == nil {
		return
	}

	if c.databases[clientConnection.DatabaseID] == nil {
		return
	}

	if c.databases[clientConnection.DatabaseID].branches[clientConnection.BranchID] == nil {
		return
	}

	for _, branchConnection := range c.databases[clientConnection.DatabaseID].branches[clientConnection.BranchID] {
		if branchConnection.connection.connection.Id() == clientConnection.connection.Id() {
			if branchConnection.connection.connection.Closed() {
				c.remove(clientConnection)
			} else {
				branchConnection.Release()
				branchConnection.lastUsedAt = time.Now().UTC()
			}

			break
		}
	}
}

// Remove a branch connection from the database group. This method is called
// without the mutex lock, so it should be called from within a mutex lock.
func (c *ConnectionManager) remove(clientConnection *ClientConnection) {
	// Remove the branch connection from the database group branch
	for i, branchConnection := range c.databases[clientConnection.DatabaseID].branches[clientConnection.BranchID] {
		if branchConnection.connection.connection.Id() == clientConnection.connection.Id() {
			c.databases[clientConnection.DatabaseID].branches[clientConnection.BranchID] = slices.Delete(c.databases[clientConnection.DatabaseID].branches[clientConnection.BranchID], i, i+1)
			break
		}
	}

	// If there are no more branches, remove the database
	if len(c.databases[clientConnection.DatabaseID].branches[clientConnection.BranchID]) == 0 {
		delete(c.databases[clientConnection.DatabaseID].branches, clientConnection.BranchID)
		c.databaseManager.Remove(clientConnection.DatabaseID, clientConnection.BranchID)
	}

	clientConnection.Close()
}

// Remove a specific client connection from the connection manager.
func (c *ConnectionManager) Remove(databaseId string, branchId string, clientConnection *ClientConnection) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.remove(clientConnection)
}

// Remove idle connections that have not been used for more than a minute.
func (c *ConnectionManager) RemoveIdleConnections() {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	for databaseId, database := range c.databases {
		var activeBranches = len(database.branches)

		for _, branchConnections := range database.branches {
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
				c.remove(branchConnection.connection)
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

// Shutdown the connection manager by closing all connections and stopping
func (c *ConnectionManager) Shutdown() {
	if c.databaseManager.systemDatabase != nil {
		err := c.databaseManager.SystemDatabase().Close()

		if err != nil {
			slog.Error("Error closing system database", "error", err)
		}
	}

	// Drain all connections
	for databaseId, database := range c.databases {
		for branchId := range database.branches {
			err := c.Drain(databaseId, branchId, func() error {
				return nil
			})

			if err != nil {
				slog.Error("Error draining connections", "error", err)
			}
		}
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	// Stop connection ticker
	if c.connectionTicker != nil {
		c.connectionTicker.Stop()
	}

	c.databases = map[string]*DatabaseGroup{}

	c.state = ConnectionManagerStateShutdown
}

// Return a state error if the connection manager is not running.
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
