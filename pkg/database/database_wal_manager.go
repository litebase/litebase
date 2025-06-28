package database

import (
	"errors"
	"fmt"
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
	connectionManager       *ConnectionManager
	DatabaseID              string
	networkFileSystem       *storage.FileSystem
	garbargeCollectionMutex *sync.RWMutex
	lastCheckpointedVersion int64
	lastTimestamp           int64 // Track last generated timestamp for uniqueness
	mutext                  *sync.RWMutex
	node                    *cluster.Node
	walIndex                *storage.WALIndex
	walVersions             map[int64]*DatabaseWAL
	walUsage                map[int64]int64
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
		BranchID:                branchId,
		checkpointing:           false,
		checkpointMutex:         &sync.Mutex{},
		connectionManager:       connectionManager,
		DatabaseID:              databaseId,
		garbargeCollectionMutex: &sync.RWMutex{},
		networkFileSystem:       networkFileSystem,
		mutext:                  &sync.RWMutex{},
		node:                    node,
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

func (w *DatabaseWALManager) Acquire() (int64, error) {
	w.mutext.Lock()
	defer w.mutext.Unlock()

	wal, err := w.GetOrCreateCurrent()

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

func (w *DatabaseWALManager) Checkpoint(wal *DatabaseWAL, fn func() error) error {
	w.checkpointMutex.Lock()
	defer w.checkpointMutex.Unlock()

	if w.checkpointing {
		return errors.New("checkpointing already in progress")
	}

	if w.node.IsReplica() {
		return errors.New("cannot set checkpointing on replica node")
	}

	if w.lastCheckpointedVersion > wal.Timestamp() {
		return errors.New("cannot set checkpointing on older version")
	}

	w.checkpointing = true
	err := wal.SetCheckpointing(true)

	if err != nil {
		w.checkpointing = false
		slog.Error("Error setting checkpointing", "error", err)
		return err
	}

	err = fn()

	if err != nil {
		w.checkpointing = false
		return err
	}

	wal.MarkCheckpointed()
	w.lastCheckpointedVersion = wal.Timestamp()

	// TODO: Broadcast checkpoint to replicas

	w.checkpointing = false

	return nil
}

func (w *DatabaseWALManager) CheckpointBarrier(fn func() error) error {
	w.checkpointMutex.Lock()
	defer w.checkpointMutex.Unlock()

	return fn()
}

// Create a new WAL version
func (w *DatabaseWALManager) Create() (*DatabaseWAL, error) {
	w.mutext.Lock()
	defer w.mutext.Unlock()

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
// timestamp that is less than or equal to the specified timestamp.
func (w *DatabaseWALManager) Get(timestamp int64) (*DatabaseWAL, error) {
	latestVersion := w.walIndex.GetClosestVersion(timestamp)

	if wal, ok := w.walVersions[latestVersion]; ok {
		return wal, nil
	}

	// No WAL exists for this timestamp - this is an error for read operations
	return nil, fmt.Errorf("no WAL version found for timestamp %d", timestamp)
}

// Get the latest WAL without creating a new one. Returns nil if no WAL exists.
func (w *DatabaseWALManager) GetLatest() (*DatabaseWAL, error) {
	w.mutext.RLock()
	defer w.mutext.RUnlock()

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

func (w *DatabaseWALManager) HasOtherActiveConnections(timestamp int64) bool {
	w.mutext.RLock()
	defer w.mutext.RUnlock()

	if _, ok := w.walUsage[timestamp]; ok {
		return w.walUsage[timestamp] > 1
	}

	return false
}

// Initialize the WAL Manager
func (w *DatabaseWALManager) init() error {
	w.mutext.Lock()
	defer w.mutext.Unlock()

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

// Check if a WAL file is in use
func (w *DatabaseWALManager) InUse(timestamp int64) bool {
	w.mutext.RLock()
	defer w.mutext.RUnlock()

	if usage, ok := w.walUsage[timestamp]; ok {
		return usage > 0
	}

	return false
}

// Get the versions of the WAL files that are in use
func (w *DatabaseWALManager) InUseVersions() []int64 {
	w.mutext.RLock()
	defer w.mutext.RUnlock()

	var versions []int64

	for version, usage := range w.walUsage {
		if usage > 0 {
			versions = append(versions, version)
		}
	}

	return versions
}

func (w *DatabaseWALManager) IsLatestVersion(timestamp int64) bool {
	// w.mutext.RLock()
	// defer w.mutext.RUnlock()

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

func (w *DatabaseWALManager) ReadAt(timestamp int64, p []byte, off int64) (n int, err error) {
	w.mutext.RLock()
	defer w.mutext.RUnlock()

	wal, err := w.Get(timestamp)

	if err != nil {
		return 0, err
	}

	return wal.ReadAt(p, off)
}

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
	w.mutext.Lock()
	defer w.mutext.Unlock()

	if _, ok := w.walUsage[timestamp]; !ok {
		return
	}

	w.walUsage[timestamp]--
}

// Run garbage collection on the WAL files
func (w *DatabaseWALManager) RunGarbageCollection() error {
	w.garbargeCollectionMutex.Lock()
	defer w.garbargeCollectionMutex.Unlock()

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

func (w *DatabaseWALManager) Shutdown() {
	w.mutext.Lock()
	defer w.mutext.Unlock()

	for _, wal := range w.walVersions {
		err := wal.Close()

		if err != nil {
			slog.Error("Failed to close WAL", "error", err)
		}
	}
}

func (w *DatabaseWALManager) Size(timestamp int64) (int64, error) {
	w.mutext.RLock()
	defer w.mutext.RUnlock()

	wal, err := w.Get(timestamp)

	if err != nil {
		return 0, err
	}

	return wal.Size()
}

func (w *DatabaseWALManager) Sync(timestamp int64) error {
	w.mutext.RLock()
	defer w.mutext.RUnlock()

	wal, err := w.Get(timestamp)

	if err != nil {
		return err
	}

	return wal.Sync()
}

func (w *DatabaseWALManager) Truncate(timestamp, size int64) error {
	w.mutext.RLock()
	defer w.mutext.RUnlock()

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
			return errors.New("no WAL versions available")
		}

		// Use the latest WAL version instead
		timestamp = latestVersion
	}

	return w.walVersions[timestamp].Truncate(size)
}

func (w *DatabaseWALManager) WaitForCheckpointing() {
	if !w.checkpointing {
		return
	}

	// timeout := time.NewTimer(3 * time.Second)

	// for {
	// 	select {
	// 	case <-timeout.C:
	// 		return
	// 	default:
	// 		if !w.checkpointing {
	// 			return
	// 		}
	// 	}

	// }
}

func (w *DatabaseWALManager) WriteAt(timestamp int64, p []byte, off int64) (n int, err error) {
	w.mutext.Lock()
	defer w.mutext.Unlock()

	wal, err := w.GetOrCreateCurrent()

	if err != nil {
		return 0, err
	}

	// Use the actual WAL timestamp, not the requested timestamp
	walTimestamp := wal.Timestamp()

	// if the wal is not the latest, panic
	if !w.IsLatestVersion(walTimestamp) {
		latest := w.getLatestVersionUnsafe()
		return 0, fmt.Errorf("cannot write to WAL file that is not the latest version: requested=%d, wal=%d, latest=%d", timestamp, walTimestamp, latest)
	}

	return wal.WriteAt(p, off)
}

// Get or create the current WAL for write operations. All connections should
// write to the same current WAL version to avoid versioning conflicts.
// Note: Caller must hold w.mutext.Lock()
func (w *DatabaseWALManager) GetOrCreateCurrent() (*DatabaseWAL, error) {
	// Always try to use the latest available WAL first
	latestVersion := w.getLatestVersionUnsafe()

	if latestVersion > 0 {
		if wal, ok := w.walVersions[latestVersion]; ok {
			// If the latest WAL hasn't been checkpointed, use it regardless of the requested timestamp
			if wal.checkpointedAt.IsZero() {
				return wal, nil
			}
		}
	}

	// If we get here, either no WAL exists or the latest WAL has been checkpointed
	// Create a new WAL using current time to ensure it's newer than any existing WAL
	newTimestamp := time.Now().UTC().UnixNano()

	// Ensure it's actually newer than the latest version
	// This handles both clock skew and rapid successive calls
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

	if latestVersion == 0 {
		slog.Info("Creating initial WAL version", "version", newTimestamp)
	} else {
		slog.Info("Creating new WAL version after checkpoint", "previous_version", latestVersion, "new_version", newTimestamp)
	}

	return w.createNew(newTimestamp)
}

// Helper method to get latest version without additional locking
// Note: Caller must already hold w.mutext lock
func (w *DatabaseWALManager) getLatestVersionUnsafe() int64 {
	var latestVersion int64
	for version := range w.walVersions {
		if version > latestVersion {
			latestVersion = version
		}
	}
	return latestVersion
}
