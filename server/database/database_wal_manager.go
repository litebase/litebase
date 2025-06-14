package database

import (
	"errors"
	"log"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/litebase/litebase/server/cluster"
	"github.com/litebase/litebase/server/cluster/messages"
	"github.com/litebase/litebase/server/storage"
)

type DatabaseWALManager struct {
	BranchId                string
	checkpointing           bool
	checkpointMutex         *sync.Mutex
	connectionManager       *ConnectionManager
	DatabaseId              string
	networkFileSystem       *storage.FileSystem
	garbargeCollectionMutex *sync.RWMutex
	lastCheckpointedVersion int64
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
		BranchId:                branchId,
		checkpointing:           false,
		checkpointMutex:         &sync.Mutex{},
		connectionManager:       connectionManager,
		DatabaseId:              databaseId,
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

func (w *DatabaseWALManager) Acquire(timestamp int64) (int64, error) {
	w.mutext.Lock()
	defer w.mutext.Unlock()

	wal, err := w.Get(timestamp)

	if err != nil {
		log.Println("Error acquiring WAL:", err)
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

	if w.node.IsReplica() {
		return errors.New("cannot set checkpointing on replica node")
	}

	if w.lastCheckpointedVersion > wal.Timestamp() {
		return errors.New("cannot set checkpointing on older version")
	}

	w.checkpointing = true
	wal.SetCheckpointing(true)

	err := fn()

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
		w.DatabaseId,
		w.BranchId,
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
		log.Println("Failed to add WAL index", err)

		return nil, err
	}

	return w.walVersions[timestamp], nil
}

// Find a WAL file for the specified timestamp. The WAL file should have a
// timestamp that is less than or equal to the specified timestamp.
func (w *DatabaseWALManager) Get(timestamp int64) (*DatabaseWAL, error) {
	latestVersion := w.walIndex.GetClosestVersion(timestamp)

	if wal, ok := w.walVersions[latestVersion]; ok {
		if wal.checkpointedAt.IsZero() {
			return wal, nil
		}
	}

	return w.createNew(timestamp)
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
		log.Println("Failed to get WAL versions", err)

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
			w.DatabaseId,
			w.BranchId,
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
	w.mutext.RLock()
	defer w.mutext.RUnlock()

	if w.node.IsPrimary() {
		return true
	}

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
		log.Println("Error creating new WAL version:", err)
		return err
	}

	w.RunGarbageCollection()

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
	// log.Println("Running garbage collection on WAL files")
	// defer log.Println("Done with garbage collection")

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
			BranchId:   w.BranchId,
			DatabaseId: w.DatabaseId,
		},
	})

	// check for errors
	for _, err := range errorMap {
		if err != nil {
			log.Println(
				errors.New("failed to get WAL version usage"),
				err,
			)
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
		log.Println("Failed to remove old WAL versions", err)

		return err
	}

	versions, err = w.walIndex.GetVersions()

	if err != nil {
		log.Println("Failed to get WAL versions", err)

		return err
	}

	// TODO: Publish the WAL index to replicas
	// _, errMap := w.node.Primary().Publish(messages.NodeMessage{
	// 	Data: messages.WALIndexMessage{
	// 		BranchId:   w.BranchId,
	// 		DatabaseId: w.DatabaseId,
	// 		Versions:   versions,
	// 	},
	// })

	// for _, err := range errMap {
	// 	if err != nil {
	// 		log.Println(
	// 			errors.New("failed to replicate WAL index"),
	// 			err,
	// 		)

	// 		return errors.New("failed to replicate WAL index")
	// 	}
	// }

	// Delete the removed WAL versions
	for _, version := range removedVersions {
		if _, ok := w.walVersions[version]; !ok {
			log.Println("WAL version not found", version)
			continue
		}

		err := w.walVersions[version].Delete()

		if err != nil {
			log.Println("Failed to delete WAL version", err)
		}

		delete(w.walVersions, version)
	}

	return nil
}

func (w *DatabaseWALManager) Shutdown() {
	w.mutext.Lock()
	defer w.mutext.Unlock()

	for _, wal := range w.walVersions {
		wal.Close()
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

	if _, ok := w.walVersions[timestamp]; !ok {
		return errors.New("WAL version not found")
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
	w.mutext.RLock()
	defer w.mutext.RUnlock()

	wal, err := w.Get(timestamp)

	if err != nil {
		return 0, err
	}

	return wal.WriteAt(p, off)
}
