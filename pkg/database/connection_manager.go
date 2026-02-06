package database

import (
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/litebase/litebase/pkg/cluster"
	"github.com/litebase/litebase/pkg/memory"
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
	connectionSize   int64 // Estimated memory per connection
	connectionTicker *time.Ticker
	databaseManager  *DatabaseManager
	databases        map[string]*DatabaseGroup
	memoryManager    *memory.Manager
	mutex            *sync.RWMutex
	state            int
}

// Checkpoint a database is necessary.
func (c *ConnectionManager) checkpoint(databaseGroup *DatabaseGroup, clientConnection *ClientConnection) bool {
	databaseGroup.mutex.Lock()
	defer databaseGroup.mutex.Unlock()

	// Skip if the last checkpoint for the database group was performed less
	// than the checkpoint threshold.
	if time.Since(databaseGroup.checkpointedAt) <= DatabaseCheckpointThreshold {
		return false
	}

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

	// Don't checkpoint if the connection manager is being drained/shutdown
	c.mutex.RLock()
	if c.state == ConnectionManagerStateDraining || c.state == ConnectionManagerStateShutdown {
		c.mutex.RUnlock()
		return
	}
	c.mutex.RUnlock()

	c.checkpointing = true

	defer func() {
		c.checkpointing = false
	}()

	c.mutex.RLock()
	databases := c.databases
	c.mutex.RUnlock()

	for _, databaseGroup := range databases {
		databaseGroup.mutex.Lock()
		needsCheckpoint := false

		for branchId := range databaseGroup.branches {
			for _, branchConnection := range databaseGroup.branches[branchId] {
				branchConnection.connection.connection.mutex.Lock()
				// Skip if the committed at time time stamp for the connection is empty
				if branchConnection.connection.connection.committedAt.IsZero() {
					branchConnection.connection.connection.mutex.Unlock()
					continue
				}

				// Skip if the committed at time stamp of the connection is before the last
				// checkpoint of the database group
				if branchConnection.connection.connection.committedAt.Before(databaseGroup.checkpointedAt) {
					branchConnection.connection.connection.mutex.Unlock()
					continue
				}

				branchConnection.connection.connection.mutex.Unlock()

				// Use the existing connection directly instead of calling c.Get()
				// which would cause a deadlock by trying to acquire c.mutex again
				if !branchConnection.Claimed() {
					branchConnection.Claim()
					needsCheckpoint = true

					go func(databaseGroup *DatabaseGroup, branchId string, bc *BranchConnection) {
						c.checkpoint(databaseGroup, bc.connection)
						bc.Release()
					}(databaseGroup, branchId, branchConnection)

					break
				}
			}
			if needsCheckpoint {
				break
			}
		}

		databaseGroup.mutex.Unlock()

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

	c.databases[databaseId].mutex.Lock()
	defer c.databases[databaseId].mutex.Unlock()

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

	// Interrupt all SQLite connections to abort any running queries
	for _, branchConnection := range databaseGroup.branches[branchId] {
		if branchConnection.connection != nil {
			dbConn := branchConnection.connection.GetConnection()

			if dbConn != nil && dbConn.sqlite3 != nil {
				dbConn.sqlite3.Interrupt()
			}
		}
	}

	wg := sync.WaitGroup{}

	for i := range databaseGroup.branches[branchId] {
		wg.Add(1)
		go func(branchConnection *BranchConnection, index int) {
			defer wg.Done()

			timeout := time.After(ConnectionDrainingWaitTime)

			select {
			case <-branchConnection.Unclaimed():
				branchConnection.connection.Close()
			case <-timeout:
				branchConnection.Close()
			}
		}(databaseGroup.branches[branchId][i], i)
	}

	c.mutex.Unlock()

	wg.Wait()

	// Remove the branch from the database group
	databaseGroup.mutex.Lock()
	defer databaseGroup.mutex.Unlock()

	return drained()
}

// Ensure that a database and branch exists in the connection manager.
func (c *ConnectionManager) ensureDatabaseBranchExists(databaseId, branchId string) {
	_, ok := c.databases[databaseId]

	if !ok {
		c.databases[databaseId] = NewDatabaseGroup()
		c.databases[databaseId].mutex.Lock()
		defer c.databases[databaseId].mutex.Unlock()
	}

	if c.databases[databaseId].branches[branchId] == nil {
		c.databases[databaseId].branches[branchId] = []*BranchConnection{}
	}
}

// Force a database to checkpoint by locking the branch and performing a checkpoint.
func (c *ConnectionManager) ForceCheckpoint(databaseId string, branchId string) error {
	connection, err := c.Get(databaseId, branchId)

	if err != nil {
		return err
	}

	defer c.Release(connection)

	c.mutex.RLock()
	databaseGroup := c.databases[databaseId]
	c.mutex.RUnlock()

	if databaseGroup == nil {
		return fmt.Errorf("database group not found")
	}

	databaseGroup.mutex.Lock()
	defer databaseGroup.mutex.Unlock()

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

	database, err := c.databaseManager.Get(databaseId)

	if err != nil {
		slog.Error("Error getting database", "error", err)

		return nil, fmt.Errorf("database '%s' not found", databaseId)
	}

	if !database.HasBranch(branchId) {
		return nil, fmt.Errorf("branch '%s' not found for database '%s'", branchId, databaseId)
	}

	// For system database, create a minimal Branch object without querying
	// to avoid circular dependency deadlock during initialization
	var branch *Branch

	if databaseId == "system" && branchId == "system" {
		branch = &Branch{
			DatabaseID:       databaseId,
			DatabaseBranchID: branchId,
			Name:             branchId,
		}
	} else {
		// Get the full branch object for non-system databases
		branch, err = database.BranchByID(branchId)

		if err != nil {
			slog.Error("Error getting database branch", "error", err)

			return nil, fmt.Errorf("branch '%s' not found for database '%s'", branchId, databaseId)
		}
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

	// Request memory for the connection
	var lease *memory.Lease

	if c.memoryManager != nil {
		lease, err = c.memoryManager.Request(c.connectionSize,
			memory.Reclaimable(false), // Connections cannot be evicted
			memory.WithPriority(memory.PriorityHigh),
			memory.WithOwner(fmt.Sprintf("connection-%s-%s", databaseId, branchId)),
		)

		if err != nil {
			return nil, fmt.Errorf("insufficient memory for connection: %w", err)
		}
	}

	// Create a new client connection, only one connection can be created at a
	// time to avoid SQL Logic errors on sqlite3_open.
	con, err := NewClientConnection(c, branch)

	if err != nil {
		// Release memory lease if connection creation fails
		if lease != nil && c.memoryManager != nil {
			err = c.memoryManager.Release(lease)

			if err != nil {
				slog.Error("Error releasing memory lease", "error", err)
			}
		}

		return nil, err
	}

	// Store the lease in the connection for later release
	con.memoryLease = lease

	databaseGroup := c.databases[databaseId]

	databaseGroup.mutex.Lock()

	databaseGroup.branches[branchId] = append(databaseGroup.branches[branchId], NewBranchConnection(
		c.cluster,
		databaseGroup,
		con,
	))

	databaseGroup.mutex.Unlock()

	return con, nil
}

// Release a client connection back to the connection manager.
func (c *ConnectionManager) Release(clientConnection *ClientConnection) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if clientConnection == nil {
		return
	}

	if c.databases[clientConnection.Branch.DatabaseID] == nil {
		return
	}

	if c.databases[clientConnection.Branch.DatabaseID].branches[clientConnection.Branch.DatabaseBranchID] == nil {
		return
	}

	for _, branchConnection := range c.databases[clientConnection.Branch.DatabaseID].branches[clientConnection.Branch.DatabaseBranchID] {
		if branchConnection.connection.connection.Id() == clientConnection.connection.Id() {
			if branchConnection.connection.connection.Closed() {
				c.remove(clientConnection)
			} else {
				// Reset skipBarriers flag to ensure connections are always in clean state for reuse
				clientConnection.connection.SetSkipBarriers(false)
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
	if c.databases[clientConnection.Branch.DatabaseID] == nil {
		return
	}

	c.databases[clientConnection.Branch.DatabaseID].mutex.Lock()

	for i, branchConnection := range c.databases[clientConnection.Branch.DatabaseID].branches[clientConnection.Branch.DatabaseBranchID] {
		if branchConnection.connection.connection.Id() == clientConnection.connection.Id() {
			c.databases[clientConnection.Branch.DatabaseID].branches[clientConnection.Branch.DatabaseBranchID] = slices.Delete(c.databases[clientConnection.Branch.DatabaseID].branches[clientConnection.Branch.DatabaseBranchID], i, i+1)
			break
		}
	}

	c.databases[clientConnection.Branch.DatabaseID].mutex.Unlock()

	// If there are no more branches, remove the database
	if len(c.databases[clientConnection.Branch.DatabaseID].branches[clientConnection.Branch.DatabaseBranchID]) == 0 {
		delete(c.databases[clientConnection.Branch.DatabaseID].branches, clientConnection.Branch.DatabaseBranchID)
		c.databaseManager.Remove(clientConnection.Branch.DatabaseID, clientConnection.Branch.DatabaseBranchID)
	}

	// Release memory lease if it exists
	if clientConnection.memoryLease != nil && c.memoryManager != nil {
		err := c.memoryManager.Release(clientConnection.memoryLease)

		if err != nil {
			slog.Error("Error releasing memory lease", "error", err)
		}

		clientConnection.memoryLease = nil
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

	// First pass: collect all branch connections that might be removable
	// (do this without calling RequiresCheckpoint to avoid nested locking)
	candidatesForRemoval := []*BranchConnection{}

	for _, database := range c.databases {
		for _, branchConnections := range database.branches {
			for _, branchConnection := range branchConnections {
				// Check basic criteria without calling RequiresCheckpoint yet
				if !branchConnection.Claimed() && time.Since(branchConnection.lastUsedAt) > DatabaseIdleTimeout {
					candidatesForRemoval = append(candidatesForRemoval, branchConnection)
				}
			}
		}
	}

	c.mutex.Unlock()

	// Second pass: check RequiresCheckpoint for each candidate (without holding ConnectionManager lock)
	actuallyRemovable := []*BranchConnection{}
	for _, branchConnection := range candidatesForRemoval {
		if !branchConnection.RequiresCheckpoint() {
			actuallyRemovable = append(actuallyRemovable, branchConnection)
		}
	}

	// Third pass: remove the connections (re-acquire lock for modifications)
	c.mutex.Lock()
	defer c.mutex.Unlock()

	for _, branchConnection := range actuallyRemovable {
		// Double-check the connection is still idle before removing
		if !branchConnection.Claimed() && time.Since(branchConnection.lastUsedAt) > DatabaseIdleTimeout {
			c.remove(branchConnection.connection)
		}
	}

	// Clean up empty databases
	for databaseId, database := range c.databases {
		activeBranches := 0
		for _, branchConnections := range database.branches {
			if len(branchConnections) > 0 {
				activeBranches++
			}
		}

		if activeBranches == 0 {
			delete(c.databases, databaseId)
		}
	}
}

// Shutdown the connection manager by closing all connections and stopping
func (c *ConnectionManager) Shutdown() {
	// Stop connection ticker first to prevent concurrent access
	if c.connectionTicker != nil {
		c.connectionTicker.Stop()
	}

	// Set state to shutdown BEFORE closing connections to prevent checkpoints
	c.mutex.Lock()
	c.state = ConnectionManagerStateShutdown
	c.mutex.Unlock()

	if c.databaseManager.systemDatabase != nil {
		err := c.databaseManager.SystemDatabase().Close()

		if err != nil {
			slog.Error("Error closing system database", "error", err)
		}
	}

	// Acquire lock before accessing databases map
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// Drain all connections
	for databaseId, database := range c.databases {
		for branchId := range database.branches {
			// Unlock during drain to avoid deadlock
			c.mutex.Unlock()
			err := c.Drain(databaseId, branchId, func() error {
				return nil
			})
			c.mutex.Lock()

			if err != nil {
				slog.Error("Error draining connections", "error", err)
			}
		}
	}

	c.databases = map[string]*DatabaseGroup{}
}

// Return a state error if the connection manager is not running.
func (c *ConnectionManager) StateError() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

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
