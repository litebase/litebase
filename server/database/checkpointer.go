package database

import (
	"errors"
	"litebase/server/backups"
	"litebase/server/storage"
	"log"
	"sync"
	"time"
)

type Checkpointer struct {
	branchId       string
	Checkpoint     *Checkpoint
	databaseId     string
	lock           sync.Mutex
	metadata       *storage.DatabaseMetadata
	rollbackLogger *backups.RollbackLogger
	snapshotLogger *backups.SnapshotLogger
	Timestamp      int64
}

type Checkpoint struct {
	Offset            int64
	LargestPageNumber int64
	Size              int64
	Timestamp         int64
}

var (
	ErrorCheckpointAlreadyInProgressError = errors.New("checkpoint already in progress")
	ErrorNoCheckpointInProgressError      = errors.New("no checkpoint in progress")
)

func NewCheckpointer(databaseId, branchId string, dfs *storage.DurableDatabaseFileSystem) (*Checkpointer, error) {
	return &Checkpointer{
		branchId:       branchId,
		databaseId:     databaseId,
		lock:           sync.Mutex{},
		metadata:       dfs.Metadata(),
		rollbackLogger: backups.NewRollbackLogger(dfs.FileSystem(), databaseId, branchId),
		snapshotLogger: backups.NewSnapshotLogger(dfs.FileSystem(), databaseId, branchId),
	}, nil
}

func (c *Checkpointer) Begin() error {
	var timestamp int64

	c.lock.Lock()
	defer c.lock.Unlock()

	if c.Checkpoint != nil {
		return ErrorCheckpointAlreadyInProgressError
	}

	if c.Timestamp == 0 {
		timestamp = time.Now().Unix()
	} else {
		timestamp = c.Timestamp
	}

	offset, size, err := c.rollbackLogger.StartFrame(timestamp)

	if err != nil {
		return err
	}

	c.Checkpoint = &Checkpoint{
		Offset:    offset,
		Size:      size,
		Timestamp: timestamp,
	}

	return nil
}

func (c *Checkpointer) CheckpointPage(pageNumber int64, data []byte) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.Checkpoint == nil {
		return ErrorNoCheckpointInProgressError
	}

	if pageNumber > c.Checkpoint.LargestPageNumber {
		c.Checkpoint.LargestPageNumber = pageNumber
	}

	size, err := c.rollbackLogger.Log(pageNumber, c.Checkpoint.Timestamp, data)

	if err != nil {
		return err
	}

	c.Checkpoint.Size += size

	return nil
}

func (c *Checkpointer) Commit() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.Checkpoint == nil {
		return ErrorNoCheckpointInProgressError
	}

	err := c.rollbackLogger.Commit(c.Checkpoint.Timestamp, c.Checkpoint.Offset, c.Checkpoint.Size)

	if err != nil {
		return err
	}

	pageCount := c.metadata.PageCount
	largestPageNumber := c.Checkpoint.LargestPageNumber

	if pageCount < largestPageNumber {
		c.metadata.SetPageCount(int64(largestPageNumber))
	}

	err = c.snapshotLogger.Log(c.Checkpoint.Timestamp, pageCount)

	if err != nil {
		log.Println("Error logging checkpoint", err)
		return err
	}

	c.Checkpoint = nil

	return nil
}

func (c *Checkpointer) Rollback() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.Checkpoint == nil {
		return ErrorNoCheckpointInProgressError
	}

	err := c.rollbackLogger.Rollback(
		c.Checkpoint.Timestamp,
		c.Checkpoint.Offset,
		c.Checkpoint.Size,
	)

	if err != nil {
		return err
	}

	c.Checkpoint = nil

	return nil
}

func (c *Checkpointer) SetTimestamp(timestamp int64) {
	c.Timestamp = timestamp
}
