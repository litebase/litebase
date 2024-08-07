package database

import (
	"context"
	"time"
)

type BranchConnection struct {
	cancel       context.CancelFunc
	context      context.Context
	connection   *ClientConnection
	inUse        bool
	lastUsedAt   time.Time
	walTimestamp int64
	walSha256    [32]byte
}

func NewBranchConnection(connection *ClientConnection, walTimestamp int64, sha256 [32]byte) *BranchConnection {
	context, cancel := context.WithCancel(context.Background())

	return &BranchConnection{
		cancel:       cancel,
		connection:   connection,
		context:      context,
		inUse:        true,
		lastUsedAt:   time.Now(),
		walSha256:    sha256,
		walTimestamp: walTimestamp,
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

func (b *BranchConnection) Unclaim() {
	b.inUse = false
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
				}
			}
		}
	}()

	return c
}
