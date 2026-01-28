package database

import (
	"context"
	"sync"
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
	mutex         sync.Mutex
	unclaimedCh   chan bool // Signaled when connection becomes unclaimed
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
		unclaimedCh:   make(chan bool, 1), // Buffered to avoid blocking
	}
}

// Claim the branch connection for use.
func (b *BranchConnection) Claim() {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	b.inUse = true
}

// Check if the branch connection is currently claimed.
func (b *BranchConnection) Claimed() bool {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return b.inUse
}

// Close the branch connection and its underlying resources.
func (b *BranchConnection) Close() {
	b.cancel()
	b.connection.Close()
}

// Release the branch connection for reuse.
func (b *BranchConnection) Release() {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	b.inUse = false

	// Signal that connection is now unclaimed (non-blocking send)
	select {
	case b.unclaimedCh <- true:
	default:
		// Channel already has a signal, no need to send another
	}
}

// Check if the branch connection requires a checkpoint to be created.
func (b *BranchConnection) RequiresCheckpoint() bool {
	b.databaseGroup.mutex.Lock()
	defer b.databaseGroup.mutex.Unlock()

	b.connection.connection.mutex.Lock()
	defer b.connection.connection.mutex.Unlock()

	return (b.databaseGroup.checkpointedAt.IsZero() && !b.connection.connection.committedAt.IsZero()) ||
		(b.connection.connection.committedAt.After(b.databaseGroup.checkpointedAt))
}

// Check if the branch connection is unclaimed, and return a channel that will
// be notified when it becomes unclaimed.
func (b *BranchConnection) Unclaimed() chan bool {
	// If already unclaimed, signal immediately
	b.mutex.Lock()

	if !b.inUse {
		b.mutex.Unlock()

		immediate := make(chan bool, 1)
		immediate <- true

		return immediate
	}

	b.mutex.Unlock()

	// Otherwise, return the channel that will be signaled on Release()
	return b.unclaimedCh
}
