package database

import (
	"errors"
	"fmt"
	"log"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/litebase/litebase/pkg/backups"
	"github.com/litebase/litebase/pkg/file"
	"github.com/litebase/litebase/pkg/storage"
)

type Checkpointer struct {
	branchId         string
	Checkpoint       *Checkpoint
	databaseId       string
	dfs              *storage.DurableDatabaseFileSystem
	sharedFileSystem *storage.FileSystem
	lock             sync.Mutex
	pageLogger       *storage.PageLogger
	rollbackLogger   *backups.RollbackLogger
	snapshotLogger   *backups.SnapshotLogger
	// New fields for atomic page capture protection
	capturedPages    map[int64]bool // Track which pages have been captured in this checkpoint
	captureMutex     sync.Mutex     // Ensure atomic page capture
	isCheckpointing  bool           // Flag to control write hook behavior
}

var (
	ErrorCheckpointAlreadyInProgressError = errors.New("checkpoint already in progress")
	ErrorNoCheckpointInProgressError      = errors.New("no checkpoint in progress")
)

// Create a new instance of the checkpointer.
func NewCheckpointer(
	databaseId,
	branchId string,
	dfs *storage.DurableDatabaseFileSystem,
	sharedFileSystem *storage.FileSystem,
	pageLogger *storage.PageLogger,
) (*Checkpointer, error) {
	cp := &Checkpointer{
		branchId:         branchId,
		databaseId:       databaseId,
		dfs:              dfs,
		sharedFileSystem: sharedFileSystem,
		lock:             sync.Mutex{},
		rollbackLogger:   backups.NewRollbackLogger(dfs.FileSystem(), databaseId, branchId),
		snapshotLogger:   backups.NewSnapshotLogger(dfs.FileSystem(), databaseId, branchId),
		pageLogger:       pageLogger,
		capturedPages:    make(map[int64]bool), // Initialize the captured pages map
	}

	err := cp.init()

	if err != nil {
		return nil, err
	}

	return cp, nil
}

// Begin starts a new checkpoint so that pages from the SQLite WAL file can be
// captured and written to a rollback log.
func (c *Checkpointer) Begin(timestamp int64) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.Checkpoint != nil {
		return ErrorCheckpointAlreadyInProgressError
	}

	offset, size, err := c.rollbackLogger.StartFrame(timestamp)

	if err != nil {
		return err
	}

	c.Checkpoint = &Checkpoint{
		BeginPageCount: c.dfs.Metadata().PageCount,
		Offset:         offset,
		Size:           size,
		Timestamp:      timestamp,
	}

	// Clear captured pages and enable checkpointing mode
	c.captureMutex.Lock()
	c.capturedPages = make(map[int64]bool)
	c.isCheckpointing = true
	c.captureMutex.Unlock()

	err = c.storeCheckpointFile()

	if err != nil {
		return err
	}

	return nil
}

// Get the path for the checkpoint file.
func (c *Checkpointer) checkPointFilePath() string {
	return fmt.Sprintf("%slogs/CHECKPOINT", file.GetDatabaseFileBaseDir(c.databaseId, c.branchId))
}

// Add a page to the checkpoint.
func (c *Checkpointer) CheckpointPage(pageNumber int64, data []byte) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.Checkpoint == nil {
		return ErrorNoCheckpointInProgressError
	}

	// Ensure we only capture each page once during the checkpoint
	c.captureMutex.Lock()
	if c.capturedPages[pageNumber] {
		c.captureMutex.Unlock()
		return nil // Page already captured, skip
	}
	c.capturedPages[pageNumber] = true
	c.captureMutex.Unlock()

	if pageNumber > c.Checkpoint.LargestPageNumber {
		c.Checkpoint.LargestPageNumber = pageNumber
	}

	size, err := c.rollbackLogger.Log(pageNumber, c.Checkpoint.Timestamp, data)

	if err != nil {
		return err
	}

	// This is absolutely wrong! We should not be writing to the page log here.
	// Need to remove it and also find a way to tombstone the pages that are
	// are added during this checkpoint.
	// _, err = c.pageLogger.Write(pageNumber, c.Checkpoint.Timestamp, data)

	// if err != nil {
	// 	return err
	// }

	c.Checkpoint.Size += int64(size)

	return nil
}

// Commit the checkpoint and remove the checkpoint file from the shared
// file system.
func (c *Checkpointer) Commit() error {
	c.lock.Lock()
	defer c.lock.Unlock()
	wg := sync.WaitGroup{}

	if c.Checkpoint == nil {
		return ErrorNoCheckpointInProgressError
	}

	// Disable checkpointing mode first
	c.captureMutex.Lock()
	c.isCheckpointing = false
	c.captureMutex.Unlock()

	var errors []error

	// Commit the rollback log that was created at the beginning of the process
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := c.rollbackLogger.Commit(c.Checkpoint.Timestamp, c.Checkpoint.Offset, c.Checkpoint.Size)

		if err != nil {
			log.Println("Error committing checkpoint", err)
			errors = append(errors, err)
		}
	}()

	pageCount := c.dfs.Metadata().PageCount
	largestPageNumber := c.Checkpoint.LargestPageNumber

	// Update the page count in the database metadata if necessary
	if pageCount < largestPageNumber {
		wg.Add(1)
		go func() {
			defer wg.Done()

			err := c.dfs.Metadata().SetPageCount(int64(largestPageNumber))

			if err != nil {
				slog.Error("Error setting page count", "error", err)
			}
		}()
	}

	// Record a new snapshot of the database
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := c.snapshotLogger.Log(time.Now().UTC().UnixNano(), pageCount)

		if err != nil {
			log.Println("Error logging checkpoint", err)
			errors = append(errors, err)
		}
	}()

	wg.Wait()

	if len(errors) > 0 {
		return fmt.Errorf("error committing checkpoint: %v", errors)
	}

	err := c.removeCheckpointFile()

	if err != nil {
		log.Println("Error removing checkpoint file", err)
	}

	c.Checkpoint = nil

	return nil
}

// When creating a new instance of the Checkpointer, we need to ensure there
// wasn't a checkpoint in progress when the database was last closed. If there
// was, we need to rollback the checkpoint since it didn't complete.
func (c *Checkpointer) init() error {
	var checkPointFileExists bool

	// Check if the checkpoint file exists
	_, err := c.sharedFileSystem.Stat(c.checkPointFilePath())

	if err != nil {
		if os.IsNotExist(err) {
			checkPointFileExists = false
		} else {
			return err
		}
	}

	if checkPointFileExists {
		// If the checkpoint file exists, read it and set the checkpoint
		data, err := c.sharedFileSystem.ReadFile(c.checkPointFilePath())

		if err != nil {
			return err
		}

		c.Checkpoint = DecodeCheckpoint(data)

		return c.Rollback()
	}

	return nil
}

// Remove the checkpoint file from the shared file system.
func (c *Checkpointer) removeCheckpointFile() error {
	return c.sharedFileSystem.Remove(c.checkPointFilePath())
}

// Rollback the Checkpointer.
func (c *Checkpointer) Rollback() error {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.Checkpoint == nil {
		return ErrorNoCheckpointInProgressError
	}

	// Disable checkpointing mode first
	c.captureMutex.Lock()
	c.isCheckpointing = false
	c.captureMutex.Unlock()

	defer func() {
		c.Checkpoint = nil
	}()

	err := c.rollbackLogger.Rollback(
		c.Checkpoint.Timestamp,
		c.Checkpoint.Offset,
		c.Checkpoint.Size,
	)

	if err != nil {
		return err
	}

	// Mark the logged pages for the checkpoint as tombstoned
	err = c.pageLogger.Tombstone(c.Checkpoint.Timestamp)

	if err != nil {
		return err
	}

	err = c.dfs.Metadata().SetPageCount(c.Checkpoint.BeginPageCount)

	if err != nil {
		slog.Error("Error setting page count", "error", err)
	}

	return c.removeCheckpointFile()
}

// Store the checkpoint file in the shared file system.
func (c *Checkpointer) storeCheckpointFile() error {
	data := c.Checkpoint.Encode()

	return c.sharedFileSystem.WriteFile(
		c.checkPointFilePath(),
		data,
		0600,
	)
}

// Run a function with the Checkpointer lock held.
func (c *Checkpointer) WithLock(fn func()) {
	c.lock.Lock()
	defer c.lock.Unlock()

	fn()
}

// IsCheckpointing returns whether a checkpoint is currently in progress
func (c *Checkpointer) IsCheckpointing() bool {
	c.captureMutex.Lock()
	defer c.captureMutex.Unlock()
	return c.isCheckpointing
}
