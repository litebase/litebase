package database

import (
	"context"
	"time"

	"github.com/litebase/litebase/pkg/cluster"
)

type BranchConnection struct {
	cancel        context.CancelFunc
	cluster       *cluster.Cluster
	context       context.Context
	connection    *ClientConnection
	databaseGroup *DatabaseGroup
	inUse         bool
	lastUsedAt    time.Time
}

// Create a new BranchConnection instance.
func NewBranchConnection(
	cluster *cluster.Cluster,
	databaseGroup *DatabaseGroup,
	connection *ClientConnection,
) *BranchConnection {
	context, cancel := context.WithCancel(context.Background())

	return &BranchConnection{
		cancel:        cancel,
		cluster:       cluster,
		connection:    connection,
		context:       context,
		databaseGroup: databaseGroup,
		inUse:         true,
		lastUsedAt:    time.Now().UTC(),
	}
}

// Claim the branch connection for use.
func (b *BranchConnection) Claim() {
	b.inUse = true
}

// Check if the branch connection is currently claimed.
func (b *BranchConnection) Claimed() bool {
	return b.inUse
}

// Close the branch connection and its underlying resources.
func (b *BranchConnection) Close() {
	b.cancel()
	b.connection.Close()
}

// Release the branch connection for reuse.
func (b *BranchConnection) Release() {
	b.inUse = false
}

// Check if the branch connection requires a checkpoint to be created.
func (b *BranchConnection) RequiresCheckpoint() bool {
	return (b.databaseGroup.checkpointedAt.IsZero() && !b.connection.connection.committedAt.IsZero()) ||
		(b.connection.connection.committedAt.After(b.databaseGroup.checkpointedAt))
}

// Check if the branch connection is unclaimed, and return a channel that will
// be notified when it becomes unclaimed.
func (b *BranchConnection) Unclaimed() chan bool {
	c := make(chan bool)

	go func() {
		for {
			select {
			case <-b.context.Done():
				return
			default:
				if !b.inUse {
					c <- true
					return
				}
			}
		}
	}()

	return c
}
