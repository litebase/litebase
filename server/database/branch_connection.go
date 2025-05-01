package database

import (
	"context"
	"time"

	"github.com/litebase/litebase/server/cluster"
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
		lastUsedAt:    time.Now(),
	}
}

func (b *BranchConnection) Claim() {
	b.inUse = true
}

func (b *BranchConnection) Claimed() bool {
	return b.inUse
}

func (b *BranchConnection) Close() {
	b.cancel()
	b.connection.Close()
}

func (b *BranchConnection) Release() {
	b.inUse = false

	b.connection.Release()
}

func (b *BranchConnection) RequiresCheckpoint() bool {
	return (b.databaseGroup.checkpointedAt.IsZero() && !b.connection.connection.committedAt.IsZero()) ||
		(b.connection.connection.committedAt.After(b.databaseGroup.checkpointedAt))
}

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
