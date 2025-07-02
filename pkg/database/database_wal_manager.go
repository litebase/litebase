package database

import (
	"errors"
	"fmt"
	"log"
	"log/slog"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/litebase/litebase/pkg/cluster"
	"github.com/litebase/litebase/pkg/cluster/messages"
	"github.com/litebase/litebase/pkg/storage"
)

type DatabaseWALManager struct {
	BranchID                string
	checkpointing           bool
	checkpointMutex         *sync.Mutex
	checkpointingWAL        *DatabaseWAL
	connectionManager       *ConnectionManager
	DatabaseID              string
	garbageCollectionMutex  *sync.RWMutex
	lastCheckpointedVersion int64
	mutex                   *sync.RWMutex
	networkFileSystem       *storage.FileSystem
	node                    *cluster.Node
	walIndex                *storage.WALIndex
	walUsage                map[int64]int64
	walVersions             map[int64]*DatabaseWAL
}

var (
	ErrCreateWALVersionOnReplica        = errors.New("cannot create WAL version on replica node")
	ErrRunWALGarbageCollectionOnReplica = errors.New("cannot run garbage collection on replica node")
)

// Create a new instance of the WAL Manager
func NewDatabaseWALManager(
	node *cluster.Node,
	connectionManager *ConnectionManager,
	databaseId,
	branchId string,
	networkFileSystem *storage.FileSystem,
) (*DatabaseWALManager, error) {
	walManager := &DatabaseWALManager{
		BranchID:               branchId,
		checkpointing:          false,
		checkpointMutex:        &sync.Mutex{},
		connectionManager:      connectionManager,
		DatabaseID:             databaseId,
		garbageCollectionMutex: &sync.RWMutex{},
		mutex:                  &sync.RWMutex{},
		networkFileSystem:      networkFileSystem,
		node:                   node,
		walIndex: storage.NewWALIndex(
			databaseId,
			branchId,
			networkFileSystem,
		),
		walUsage:    make(map[int64]int64),
		walVersions: make(map[int64]*DatabaseWAL),
	}

	err := walManager.init()

	return walManager, err
}

// Acquire a WAL version for use. This safely creates a new WAL if one does not
// already exists or it returns the latest WAL version
func (w *DatabaseWALManager) Acquire() (int64, error) {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	wal, err := w.getOrCreateCurrent()

	if err != nil {
		slog.Error("Error acquiring WAL", "error", err)
		return 0, err
	}

	if _, ok := w.walUsage[wal.timestamp]; !ok {
		w.walUsage[wal.timestamp] = 1
	} else {
		w.walUsage[wal.timestamp]++
	}

	return wal.Timestamp(), nil
}

// Checkpoint the WAL version. This ensures the current WAL is checkpointed
// atomically and does not allow other operations to interfere with the
// checkpointing process
func (w *DatabaseWALManager) Checkpoint(fn func(wal *DatabaseWAL) error) error {
	w.checkpointMutex.Lock()
	defer w.checkpointMutex.Unlock()

	if w.checkpointing {
		return errors.New("checkpointing already in progress")
	}

	if w.node.IsReplica() {
		return errors.New("cannot set checkpointing on replica node")
	}

	// Get the latest WAL for the database without creating a new one
	wal, err := w.getLatestUnsafe()

	if err != nil {
		log.Println("Error acquiring latest WAL:", err)
		return err
	}

	// If no WAL exists, there's nothing to checkpoint
	if wal == nil {
		return nil
	}

	if w.lastCheckpointedVersion > wal.Timestamp() {
		return errors.New("cannot set checkpointing on older version")
	}

	// Mark the WAL as being checkpointed - this will force new connections to create a new WAL
	err = wal.SetCheckpointing(true)

	if err != nil {
		slog.Error("Error setting checkpointing", "error", err)
		return err
	}

	// Set checkpoint state
	w.checkpointing = true
	w.checkpointingWAL = wal

	defer func() {
		w.checkpointing = false
		w.checkpointingWAL = nil
	}()

	// Execute the checkpoint function
	err = fn(wal)

	if err != nil {
		return err
	}

	wal.MarkCheckpointed()
	w.lastCheckpointedVersion = wal.Timestamp()

	// TODO: Broadcast checkpoint to replicas

	return nil
}

// CheckpointBarrier is a conviencene method to insure that transactions are
// operating in a consisten state around checkpoints to prevent database
// corruption or other state inconsistencies
func (w *DatabaseWALManager) CheckpointBarrier(fn func() error) error {
	w.checkpointMutex.Lock()
	defer w.checkpointMutex.Unlock()

	return fn()
}

// Create a new WAL version
func (w *DatabaseWALManager) Create() (*DatabaseWAL, error) {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	return w.createNew(time.Now().UTC().UnixNano())
}

// Create a new WAL version
func (w *DatabaseWALManager) createNew(timestamp int64) (*DatabaseWAL, error) {
	if w.node.IsReplica() {
		return nil, ErrCreateWALVersionOnReplica
	}

	// Add the new version
	w.walVersions[timestamp] = NewDatabaseWAL(
		w.node,
		w.connectionManager,
		w.DatabaseID,
		w.BranchID,
		w.networkFileSystem,
		w,
		timestamp,
	)

	w.walUsage[timestamp] = 0

	// Update the WAL index
	var versionNumbers = make([]int64, len(w.walVersions))

	var index = 0

	for version := range w.walVersions {
		versionNumbers[index] = version
		index++
	}

	err := w.walIndex.SetVersions(versionNumbers)

	if err != nil {
		slog.Error("Failed to add WAL index", "error", err)

		return nil, err
	}

	return w.walVersions[timestamp], nil
}

// Find a WAL file for the specified timestamp. The WAL file should have a
// timestamp that is less than or equal to the specified timestamp
func (w *DatabaseWALManager) Get(timestamp int64) (*DatabaseWAL, error) {
	latestVersion := w.walIndex.GetClosestVersion(timestamp)

	if wal, ok := w.walVersions[latestVersion]; ok {
		return wal, nil
	}

	// No WAL exists for this timestamp - this is an error for read operations
	return nil, fmt.Errorf("no WAL version found for timestamp %d", timestamp)
}

// Get the latest WAL without creating a new one. Returns nil if no WAL exists
func (w *DatabaseWALManager) GetLatest() (*DatabaseWAL, error) {
	w.mutex.RLock()
	defer w.mutex.RUnlock()

	return w.getLatestUnsafe()
}

// getLatestUnsafe gets the latest WAL without acquiring locks
// Note: Caller must hold w.mutex.RLock() or w.mutex.Lock()
func (w *DatabaseWALManager) getLatestUnsafe() (*DatabaseWAL, error) {
	var latestVersion int64
	var found bool

	for version := range w.walVersions {
		if version > latestVersion {
			latestVersion = version
			found = true
		}
	}

	if !found {
		return nil, nil // No WAL exists, nothing to checkpoint
	}

	if wal, ok := w.walVersions[latestVersion]; ok {
		return wal, nil
	}

	return nil, errors.New("latest WAL version not found")
}

// Initialize the WAL Manager
func (w *DatabaseWALManager) init() error {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	versions, err := w.walIndex.GetVersions()

	if err != nil {
		slog.Error("Failed to get WAL versions", "error", err)

		return err
	}

	var latestTimestamp int64

	for _, version := range versions {
		if version > latestTimestamp {
			latestTimestamp = version
		}

		w.walVersions[version] = NewDatabaseWAL(
			w.node,
			w.connectionManager,
			w.DatabaseID,
			w.BranchID,
			w.networkFileSystem,
			w,
			version,
		)

		w.walUsage[version] = 0
	}

	return nil
}

// Check if a WAL version is currently in use
func (w *DatabaseWALManager) InUse(timestamp int64) bool {
	w.mutex.RLock()
	defer w.mutex.RUnlock()

	if usage, ok := w.walUsage[timestamp]; ok {
		return usage > 0
	}

	return false
}

// Get all of the versions of the WAL files that are actively in use
func (w *DatabaseWALManager) InUseVersions() []int64 {
	w.mutex.RLock()
	defer w.mutex.RUnlock()

	var versions []int64

	for version, usage := range w.walUsage {
		if usage > 0 {
			versions = append(versions, version)
		}
	}

	return versions
}

// Check if the specified timestamp is the latest version of the WAL
func (w *DatabaseWALManager) IsLatestVersion(timestamp int64) bool {
	w.mutex.RLock()
	defer w.mutex.RUnlock()

	// if w.node.IsPrimary() {
	// 	return true
	// }

	if _, ok := w.walVersions[timestamp]; !ok {
		if w.node.IsReplica() {
			return false
		}

		return false
	}

	var latestVersion int64

	for version := range w.walVersions {
		if version > latestVersion {
			latestVersion = version
		}
	}

	return latestVersion == timestamp
}

// Read from a WAL log file that corresponds to the specified timestamp
func (w *DatabaseWALManager) ReadAt(timestamp int64, p []byte, off int64) (n int, err error) {
	w.mutex.RLock()
	defer w.mutex.RUnlock()

	wal, err := w.Get(timestamp)

	if err != nil {
		return 0, err
	}

	return wal.ReadAt(p, off)
}

// Refresh the WAL manager by creating a new WAL version and running garbage
// collection on the WAL files
func (w *DatabaseWALManager) Refresh() error {
	_, err := w.Create()

	if err != nil {
		slog.Error("Error creating new WAL version", "error", err)
		return err
	}

	err = w.RunGarbageCollection()

	if err != nil {
		slog.Error("Error running garbage collection", "error", err)
	}

	return nil
}

// Release a WAL file from use
func (w *DatabaseWALManager) Release(timestamp int64) {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	if _, ok := w.walUsage[timestamp]; !ok {
		return
	}

	w.walUsage[timestamp]--
}

// Run garbage collection on the WAL files
func (w *DatabaseWALManager) RunGarbageCollection() error {
	w.garbageCollectionMutex.Lock()
	defer w.garbageCollectionMutex.Unlock()

	if w.node.IsReplica() {
		return ErrRunWALGarbageCollectionOnReplica
	}

	// Get all the versions of the WAL Index
	versions, err := w.walIndex.GetVersions()

	if err != nil {
		return err
	}

	// Check the in use versions and get the earliest timestamp
	var inUseVersions []int64

	for version, usage := range w.walUsage {
		if usage > 0 {
			inUseVersions = append(inUseVersions, version)
		}
	}

	// Also protect the WAL that is currently being checkpointed
	if w.checkpointingWAL != nil {
		checkpointingTimestamp := w.checkpointingWAL.Timestamp()
		inUseVersions = append(inUseVersions, checkpointingTimestamp)
		slog.Debug("Protecting checkpointing WAL from garbage collection", "timestamp", checkpointingTimestamp)
	}

	earliestInUseVersion := int64(0)

	for _, version := range inUseVersions {
		if earliestInUseVersion == 0 || version < earliestInUseVersion {
			earliestInUseVersion = version
		}
	}

	// Retain any versions that fall after the earliest in use version
	for i := len(versions) - 1; i >= 0; i-- {
		if versions[i] >= earliestInUseVersion && earliestInUseVersion != 0 {
			versions = slices.Delete(versions, i, i+1)
		}
	}

	// Ask replicas for their oldest known timestamps
	responseMap, errorMap := w.node.Primary().Publish(messages.NodeMessage{
		Data: messages.WALVersionUsageRequest{
			BranchID:   w.BranchID,
			DatabaseID: w.DatabaseID,
		},
	})

	// check for errors
	for _, err := range errorMap {
		if err != nil {
			slog.Error("failed to get WAL version usage", "error", err)
		}
	}

	// get the oldest timestamps from the responses
	var oldestReplicaVersion int64

	for _, response := range responseMap {
		if response.(messages.NodeMessage).Data == nil {
			continue
		}

		message := response.(messages.NodeMessage).Data.(messages.WALVersionUsageResponse)

		for _, version := range message.Versions {
			if oldestReplicaVersion == 0 || version < oldestReplicaVersion {
				oldestReplicaVersion = version
			}
		}
	}

	// Retain any versions that fall after the oldest replica version
	for i := len(versions) - 1; i >= 0; i-- {
		if versions[i] >= oldestReplicaVersion && oldestReplicaVersion != 0 {
			versions = slices.Delete(versions, i, i+1)
		}
	}

	if len(versions) == 0 {
		return nil
	}

	var timestamp int64

	// Sort the versions in descending order
	sort.Slice(versions, func(i, j int) bool {
		return versions[i] > versions[j]
	})

	timestamp = versions[0]

	// Remove old WAL versions from the index
	removedVersions, err := w.walIndex.RemoveVersionsFrom(timestamp)

	if err != nil {
		slog.Error("Failed to remove old WAL versions", "error", err)

		return err
	}

	versions, err = w.walIndex.GetVersions()

	if err != nil {
		slog.Error("Failed to get WAL versions", "error", err)

		return err
	}

	// TODO: Publish the WAL index to replicas
	// _, errMap := w.node.Primary().Publish(messages.NodeMessage{
	// 	Data: messages.WALIndexMessage{
	// 		BranchID:   w.BranchID,
	// 		DatabaseID: w.DatabaseID,
	// 		Versions:   versions,
	// 	},
	// })

	// for _, err := range errMap {
	// 	if err != nil {
	// 		slog.Error("failed to replicate WAL index", "error", err)

	// 		return errors.New("failed to replicate WAL index")
	// 	}
	// }

	// Delete the removed WAL versions
	for _, version := range removedVersions {
		if _, ok := w.walVersions[version]; !ok {
			slog.Error("WAL version not found", "version", version)
			continue
		}

		err := w.walVersions[version].Delete()

		if err != nil {
			slog.Error("Failed to delete WAL version", "error", err)
		}

		delete(w.walVersions, version)
	}

	return nil
}

// Shutdown the WAL manager and close all WAL files
func (w *DatabaseWALManager) Shutdown() {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	for _, wal := range w.walVersions {
		err := wal.Close()

		if err != nil {
			slog.Error("Failed to close WAL", "error", err)
		}
	}
}

// Size returns the size of the WAL file for the specified timestamp
func (w *DatabaseWALManager) Size(timestamp int64) (int64, error) {
	w.mutex.RLock()
	defer w.mutex.RUnlock()

	wal, err := w.Get(timestamp)

	if err != nil {
		return 0, err
	}

	return wal.Size()
}

// Sync the WAL file for the specified timestamp
func (w *DatabaseWALManager) Sync(timestamp int64) error {
	w.mutex.RLock()
	defer w.mutex.RUnlock()

	wal, err := w.Get(timestamp)

	if err != nil {
		return err
	}

	return wal.Sync()
}

// Truncate the WAL file for the specified timestamp to the given size
func (w *DatabaseWALManager) Truncate(timestamp, size int64) error {
	w.mutex.RLock()
	defer w.mutex.RUnlock()

	// If the specific timestamp doesn't exist, use the latest WAL
	if _, ok := w.walVersions[timestamp]; !ok {
		// Find the latest WAL version
		var latestVersion int64
		for version := range w.walVersions {
			if version > latestVersion {
				latestVersion = version
			}
		}
		if latestVersion == 0 {
			log.Println("walversions", w.walVersions)
			return errors.New("no WAL versions available")
		}

		// Use the latest WAL version instead
		timestamp = latestVersion
	}

	return w.walVersions[timestamp].Truncate(size)
}

func (w *DatabaseWALManager) WriteAt(timestamp int64, p []byte, off int64) (n int, err error) {
	w.mutex.Lock()
	defer w.mutex.Unlock()

	// Get the latest WAL for the database without creating a new one.
	wal, err := w.getLatestUnsafe()

	if err != nil {
		log.Println("Error acquiring latest WAL:", err)
		return 0, err
	}

	if wal.Timestamp() != timestamp {
		return 0, fmt.Errorf("cannot write to WAL, the version is not the latest: requested=%d, wal=%d, latest=%d", timestamp, wal.Timestamp(), w.getLatestVersionUnsafe())
	}

	return wal.WriteAt(p, off)
}

// getOrCreateCurrent gets the current WAL or creates a new one if needed. All connections should
// write to the same current WAL version to avoid versioning conflicts.
// Note: Caller must hold w.mutex.Lock()
func (w *DatabaseWALManager) getOrCreateCurrent() (*DatabaseWAL, error) {
	// Always try to use the latest available WAL first
	latestVersion := w.getLatestVersionUnsafe()

	// Check if we have a valid WAL to use
	if latestVersion > 0 {
		if wal, ok := w.walVersions[latestVersion]; ok {
			// Don't use a WAL that's currently being checkpointed or has been checkpointed
			if wal.checkpointedAt.IsZero() && !wal.IsCheckpointing() {
				return wal, nil
			}
		}
	}

	// If we get here, either no WAL exists or the latest WAL has been checkpointed
	// Need to create a new WAL

	// Create a new WAL using current time to ensure it's newer than any existing WAL
	newTimestamp := time.Now().UTC().UnixNano()

	// Ensure it's actually newer than the latest version
	if newTimestamp <= latestVersion {
		newTimestamp = latestVersion + 1
	}

	// Additional safety: ensure timestamp is strictly increasing even for concurrent calls
	for {
		if _, exists := w.walVersions[newTimestamp]; !exists {
			break // This timestamp is available
		}

		newTimestamp++ // Increment until we find an available timestamp
	}

	return w.createNew(newTimestamp)
}

// Helper method to get latest version without additional locking
// Note: Caller must already hold w.mutex lock
func (w *DatabaseWALManager) getLatestVersionUnsafe() int64 {
	var latestVersion int64
	for version := range w.walVersions {
		if version > latestVersion {
			latestVersion = version
		}
	}
	return latestVersion
}
