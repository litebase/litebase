package database

import (
	"fmt"
	"litebase/server/file"
	"litebase/server/node"
	"log"
	"os"
	"sync"
	"time"

	"github.com/klauspost/compress/s2"
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
			mutext:    &sync.RWMutex{},
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
	if clientConnection.connection.committedAt.IsZero() || clientConnection.connection.committedAt.Before(databaseGroup.checkpointedAt) {
		return false
	}

	// Skip if the database connection is before the checkpoint time
	if clientConnection.connection.committedAt.IsZero() || clientConnection.connection.committedAt.Before(databaseGroup.checkpointedAt) {
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
		// log.Println("Error checkpointing database", err)
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

func (c *ConnectionManagerInstance) CheckpointReplica(databaseUuid, branchUuid string, timestamp int64) {
	c.mutext.Lock()
	c.ensureDatabaseBranchExists(databaseUuid, branchUuid)
	c.databases[databaseUuid].branchWalSha256[branchUuid] = [32]byte{}
	c.databases[databaseUuid].branchWalTimestamps[branchUuid] = timestamp
	c.mutext.Unlock()
}

func (c *ConnectionManagerInstance) Drain(databaseUuid string, branchUuid string, drained func() error) error {
	c.mutext.Lock()

	databaseGroup, ok := c.databases[databaseUuid]

	if !ok {
		defer c.mutext.Unlock()

		return drained()
	}

	// TODO: This is causing a deadlock
	// databaseGroup.lockMutex.Lock()
	// defer databaseGroup.lockMutex.Unlock()

	_, ok = databaseGroup.branches[branchUuid]

	if !ok {
		defer c.mutext.Unlock()

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

	c.mutext.Unlock()

	// // Wait for all connections to close
	// var retries = 0

	// // Wait for all BranchConnection <-Unclaimed() to be true
	// for {
	// 	log.Println("retries", retries)
	// 	if len(databaseGroup.branches[branchUuid]) == 0 || retries > 100 {
	// 		break
	// 	}

	// 	time.Sleep(10 * time.Millisecond)

	// 	c.mutext.Lock()
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
	// 	c.mutext.Unlock()

	// 	retries++
	// }

	// c.mutext.Lock()
	// defer c.mutext.Unlock()

	// // Force close all connections
	// for _, branchConnection := range databaseGroup.branches[branchUuid] {
	// 	branchConnection.connection.Close()
	// }

	// Remove the branch from the database group
	databaseGroup.lockMutex.Lock()
	defer databaseGroup.lockMutex.Unlock()

	delete(databaseGroup.branches, branchUuid)
	delete(databaseGroup.branchWalSha256, branchUuid)

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
		c.databases[databaseUuid].branchWalSha256[branchUuid] = [32]byte{}
		c.databases[databaseUuid].branchWalTimestamps[branchUuid] = 0
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

	c.mutext.Lock()
	defer c.mutext.Unlock()

	// if c.databases[databaseUuid] != nil {
	// 	c.databases[databaseUuid].lockMutex.Lock()
	// 	defer c.databases[databaseUuid].lockMutex.Unlock()
	// }

	if c.databases[databaseUuid] != nil &&
		c.databases[databaseUuid].branches[branchUuid] != nil &&
		len(c.databases[databaseUuid].branches[branchUuid]) > 0 {
		for _, branchConnection := range c.databases[databaseUuid].branches[branchUuid] {
			if !branchConnection.Claimed() {
				// if branchConnection.walSha256 != c.databases[databaseUuid].branchWalSha256[branchUuid] {
				// 	// Remove the branch connection from the database group branch
				// 	c.remove(databaseUuid, branchUuid, branchConnection.connection)
				// 	log.Println("WAL sha256 mismatch, removing connection", branchConnection.walSha256, c.databases[databaseUuid].branchWalSha256[branchUuid])

				// 	continue
				// }

				branchConnection.Claim()
				// log.Println("Get connection", len(c.databases[databaseUuid].branches[branchUuid]))

				return branchConnection.connection, nil
			}
		}
	}

	c.ensureDatabaseBranchExists(databaseUuid, branchUuid)

	// Retrieve the WAL from the primary
	err := c.retrieveWal(databaseUuid, branchUuid)

	if err != nil {
		// log.Println("ERROR retrieving wal", err)
		return nil, err
	}

	walTimestamp := c.databases[databaseUuid].branchWalTimestamps[branchUuid]

	// Create a new client connection, only one connection can be created at a
	// time to avoid SQL Logic errors on sqlite3_open.
	con, err := NewClientConnection(databaseUuid, branchUuid, walTimestamp)

	if err != nil {
		return nil, err
	}

	c.databases[databaseUuid].branches[branchUuid] = append(c.databases[databaseUuid].branches[branchUuid], NewBranchConnection(
		con,
		walTimestamp,
		c.databases[databaseUuid].branchWalSha256[branchUuid],
	))

	return con, nil
}

func (c *ConnectionManagerInstance) Release(databaseUuid string, branchUuid string, clientConnection *ClientConnection) {
	c.mutext.Lock()
	defer c.mutext.Unlock()

	if c.databases[databaseUuid] == nil {
		return
	}

	for _, branchConnection := range c.databases[databaseUuid].branches[branchUuid] {
		if branchConnection.connection.connection.Id() == clientConnection.connection.Id() {
			// if branchConnection.walSha256 != c.databases[databaseUuid].branchWalSha256[branchUuid] {
			// 	// Remove the branch connection from the database group branch
			// 	c.remove(databaseUuid, branchUuid, clientConnection)

			// 	return
			// }

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
		delete(c.databases[databaseUuid].branchWalSha256, branchUuid)
		delete(c.databases[databaseUuid].branchWalTimestamps, branchUuid)
		DatabaseResources().Remove(databaseUuid, branchUuid)
	}

	clientConnection.Close()
}

func (c *ConnectionManagerInstance) Remove(databaseUuid string, branchUuid string, clientConnection *ClientConnection) {
	c.mutext.Lock()
	defer c.mutext.Unlock()

	c.remove(databaseUuid, branchUuid, clientConnection)
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
				if !branchConnection.Claimed() && time.Since(branchConnection.lastUsedAt) > 1*time.Minute {
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

func (c *ConnectionManagerInstance) retrieveWal(databaseUuid, branchUuid string) error {
	if node.Node().IsPrimary() {
		return nil
	}

	databaseGroup := c.databases[databaseUuid]

	if databaseGroup == nil {
		return fmt.Errorf("database group not found")
	}

	responses, err := node.Node().SendWithStreamingResonse(
		node.NodeMessage{
			Id:   fmt.Sprintf("wal:%s", file.DatabaseHash(databaseUuid, branchUuid)),
			Type: "WALMessage",
			Data: node.WALMessage{
				DatabaseUuid: databaseUuid,
				BranchUuid:   branchUuid,
			},
		},
	)

	if err != nil {
		log.Println("Failed to retrieve wal from primary: ", err)
		return err
	}

	// TODO: The following causes a deadlock
	lock := databaseGroup.locks[branchUuid]

	// Lock the branch to allow the WAL to be written
	lock.Lock()

	defer lock.Unlock()

	var walFile *os.File
	var fileSha256 [32]byte
	var timestamp int64

	for response := range responses {
		if response.Type == "Error" {
			log.Println("Error retrieving wal from primary: ", response.Error)
			return fmt.Errorf(response.Error)
		}

		if response.Type != "WALMessageResponse" {
			log.Println("Unexpected response from primary: ", response.Type)
			return fmt.Errorf("unexpected response from primary")
		}

		walMessageResponse := response.Data.(node.WALMessageResponse)
		lastChunk := walMessageResponse.LastChunk

		if walFile == nil {
			walFile, err = os.OpenFile(
				WalVersionPath(databaseUuid, branchUuid, walMessageResponse.Timestamp),
				os.O_RDWR|os.O_CREATE,
				0644,
			)

			if err != nil {
				log.Println("Failed to open WAL file: ", err)
				return err
			}

			defer walFile.Close()

			// Truncate the file
			if err := walFile.Truncate(0); err != nil {
				log.Println("Failed to truncate WAL file: ", err)
				return err
			}
		}

		decompressed, err := s2.Decode(nil, walMessageResponse.Data)

		if err != nil {
			log.Println("Failed to decode WAL chunk: ", err)
			return err
		}

		_, err = walFile.Write(decompressed)

		if err != nil {
			log.Println("Failed to write WAL file: ", err)
			return err
		}

		if lastChunk {
			fileSha256 = walMessageResponse.Sha256
			timestamp = walMessageResponse.Timestamp
			break
		}
	}

	// if walFile == nil {
	// 	return nil
	// }

	// hasher := sha256.New()

	// walFile.Seek(0, 0)

	// if _, err := walFile.WriteTo(hasher); err != nil {
	// return err
	// }

	// var updatedFileSha256 [32]byte

	// copy(updatedFileSha256[:], hasher.Sum(nil))

	// if fileSha256 != updatedFileSha256 {
	// 	// TODO: Delete the wal and pull the latest from the primary or remote storage
	// 	log.Println("sha256 mismatch")
	// 	return nil
	// }

	c.databases[databaseUuid].branchWalSha256[branchUuid] = fileSha256
	c.databases[databaseUuid].branchWalTimestamps[branchUuid] = timestamp

	return nil
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

func (c *ConnectionManagerInstance) UpdateWal(
	databaseUuid, branchUuid string,
	fileSha256 [32]byte,
	timestamp int64,
) error {
	c.mutext.Lock()
	c.ensureDatabaseBranchExists(databaseUuid, branchUuid)
	c.databases[databaseUuid].branchWalSha256[branchUuid] = fileSha256
	c.databases[databaseUuid].branchWalTimestamps[branchUuid] = timestamp
	c.mutext.Unlock()

	return nil
}

func (c *ConnectionManagerInstance) Tick() {
	c.CheckpointAll()
	c.RemoveIdleConnections()
}

func (c *ConnectionManagerInstance) WalTimestamp(databaseUuid, branchUuid string) (int64, error) {
	c.mutext.Lock()
	defer c.mutext.Unlock()

	c.ensureDatabaseBranchExists(databaseUuid, branchUuid)

	return c.databases[databaseUuid].branchWalTimestamps[branchUuid], nil
}
