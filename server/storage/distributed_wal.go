package storage

import (
	"sync"
	"time"
)

// A DistributedWal is a write-ahead log that is distributed across multiple nodes.
// The primary database can write to the DistributedWal to replicate changes to
// the database replicas.
type DistributedWal struct {
	BranchId   string
	DatabaseId string
	mutex      *sync.Mutex
	replicator DistributedWalReplicator
	sequence   int64
	timestamp  int64
}

type DistributedWalReplicator interface {
	WriteAt(databaseId, branchId string, p []byte, off, sequence, timestamp int64) error
	Truncate(databaseId, branchId string, size, sequence, timestamp int64) error
}

func NewDistributedWal(
	databaseId string,
	branchId string,
	replicator DistributedWalReplicator,
) *DistributedWal {
	return &DistributedWal{
		BranchId:   branchId,
		DatabaseId: databaseId,
		mutex:      &sync.Mutex{},
		replicator: replicator,
		sequence:   0,
		timestamp:  time.Now().UnixNano(),
	}
}

// Close the DistributedWal.
func (d *DistributedWal) Close() error {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	return nil
}

// Truncate the DistributedWal to the given size.
func (d *DistributedWal) Truncate(size int64) error {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	d.sequence = 0
	d.timestamp = time.Now().UnixNano()

	go func(sequence, timestamp int64) {
		d.replicator.Truncate(d.DatabaseId, d.BranchId, size, sequence, timestamp)
	}(d.sequence, d.timestamp)

	return nil
}

// WriteAt writes len(p) bytes from p to the DistributedWal at the given offset.
func (d *DistributedWal) WriteAt(p []byte, off int64) error {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	var err error

	d.sequence++
	d.timestamp = time.Now().UnixNano()

	d.replicator.WriteAt(d.DatabaseId, d.BranchId, p, off, d.sequence, d.timestamp)

	return err
}
