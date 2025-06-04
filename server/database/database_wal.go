package database

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	internalStorage "github.com/litebase/litebase/internal/storage"
	"github.com/litebase/litebase/server/cache"
	"github.com/litebase/litebase/server/cluster"
	"github.com/litebase/litebase/server/file"
	"github.com/litebase/litebase/server/storage"
)

var (
	DatabaseWALSyncInterval = 100 * time.Millisecond
)

type DatabaseWAL struct {
	BranchId       string
	cache          *cache.LFUCache
	createdAt      time.Time
	DatabaseId     string
	checkpointedAt time.Time
	checkpointing  bool
	file           internalStorage.File
	fileSystem     *storage.FileSystem
	hash           string
	lastKnownSize  int64
	lastSyncTime   time.Time
	lastWriteTime  time.Time
	mutex          *sync.RWMutex
	node           *cluster.Node
	Path           string
	syncMutex      *sync.Mutex
	timestamp      int64
	walManager     *DatabaseWALManager
}

func NewDatabaseWAL(
	node *cluster.Node,
	connectionManager *ConnectionManager,
	databaseId string,
	branchId string,
	fileSystem *storage.FileSystem,
	walManager *DatabaseWALManager,
	timestamp int64,
) *DatabaseWAL {
	return &DatabaseWAL{
		BranchId:      branchId,
		cache:         cache.NewLFUCache(1000),
		createdAt:     time.Now(),
		DatabaseId:    databaseId,
		fileSystem:    fileSystem,
		lastKnownSize: -1,
		lastSyncTime:  time.Time{},
		mutex:         &sync.RWMutex{},
		node:          node,
		Path:          fmt.Sprintf("%slogs/wal/WAL_%d", file.GetDatabaseFileBaseDir(databaseId, branchId), timestamp),
		syncMutex:     &sync.Mutex{},
		timestamp:     timestamp,
		walManager:    walManager,
	}
}

func (wal *DatabaseWAL) Checkpointing() bool {
	return wal.checkpointing
}

func (wal *DatabaseWAL) Close() error {
	if wal.file != nil {
		err := wal.file.Close()

		if err != nil {
			log.Println(err)
		}

		wal.file = nil
	}

	return nil
}

func (wal *DatabaseWAL) Delete() error {
	wal.mutex.Lock()
	defer wal.mutex.Unlock()

	if wal.node.IsReplica() {
		return errors.New("cannot delete WAL file on replica node")
	}

	file, err := wal.File()

	if err != nil {
		log.Println(err)
		return err
	}

	err = file.Close()

	if err != nil {
		log.Println(err)
		return err
	}

	err = wal.fileSystem.Remove(wal.Path)

	if err != nil {
		log.Println(err)
		return err
	}

	return nil
}

func (wal *DatabaseWAL) File() (internalStorage.File, error) {
	if wal.file != nil {
		return wal.file, nil
	}

tryOpen:
	file, err := wal.fileSystem.OpenFileDirect(
		wal.Path,
		os.O_CREATE|os.O_RDWR,
		0644,
	)

	if err != nil {
		if os.IsNotExist(err) {
			err = wal.fileSystem.MkdirAll(filepath.Dir(wal.Path), 0755)

			if err != nil {
				return nil, err
			}

			goto tryOpen
		} else {
			return nil, err
		}
	}

	wal.file = file

	return wal.file, nil
}

func (wal *DatabaseWAL) Hash() string {
	if wal.hash != "" {
		return wal.hash
	}

	checksum := sha256.Sum256(fmt.Appendf(nil, "%s:%s:%d", wal.DatabaseId, wal.BranchId, wal.Timestamp()))
	wal.hash = hex.EncodeToString(checksum[:])

	return wal.hash
}

func (wal *DatabaseWAL) IsCheckpointed() bool {
	wal.mutex.RLock()
	defer wal.mutex.RUnlock()

	return !wal.checkpointedAt.IsZero()
}

func (wal *DatabaseWAL) MarkCheckpointed() {
	wal.mutex.Lock()
	defer wal.mutex.Unlock()

	wal.checkpointing = false
	wal.checkpointedAt = time.Now()
}

func (wal *DatabaseWAL) performAsynchronousSync() {
	go func() {
		if !wal.syncMutex.TryLock() {
			return
		}

		defer wal.syncMutex.Unlock()
		start := time.Now()

		wal.mutex.Lock()

		defer func() {
			wal.mutex.Unlock()
			log.Println("Async WAL file sync took", time.Since(start))
		}()

		file, err := wal.File()

		if err != nil {
			log.Println(err)
			return
		}

		err = file.Sync()

		if err != nil {
			log.Println(err)
			return
		}

		wal.lastSyncTime = time.Now()
	}()
}

func (wal *DatabaseWAL) ReadAt(p []byte, off int64) (n int, err error) {
	// start := time.Now()

	wal.mutex.Lock()
	defer wal.mutex.Unlock()

	cacheKey := fmt.Sprintf("%d", off)

	if data, found := wal.cache.Get(cacheKey); found && len(data.([]byte)) == len(p) {
		// defer func() {
		// 	log.Println("WAL file read", off, time.Since(start))
		// }()
		return copy(p, data.([]byte)), nil
	}

	file, err := wal.File()

	if err != nil {
		log.Println(err)
		return 0, err
	}

	if wal.node.IsPrimary() && !wal.checkpointedAt.IsZero() {
		panic(fmt.Sprintf("WAL file has been checkpointed, cannot read from it - %d", wal.timestamp))
	}

	if wal.node.IsReplica() && !wal.checkpointedAt.IsZero() {
		panic(fmt.Sprintf("WAL file has been checkpointed, cannot read from it - %d", wal.timestamp))
	}

	n, err = file.ReadAt(p, off)

	if err != nil {
		return n, err
	}

	// Cache the read data
	// if n == int(wal.walManager.connectionManager.cluster.Config.PageSize) {
	wal.cache.Put(cacheKey, slices.Clone(p))
	// }

	return n, nil
}

func (wal *DatabaseWAL) RequiresCheckpoint() bool {
	if wal.lastKnownSize < 0 {
		wal.Size()
	}

	return wal.checkpointedAt.IsZero() && (wal.lastKnownSize > 0 || !wal.lastWriteTime.IsZero())
}

func (wal *DatabaseWAL) SetCheckpointing(checkpointing bool) error {
	if wal.node.IsReplica() {
		return errors.New("cannot set checkpointing on replica node")
	}

	wal.checkpointing = checkpointing

	return nil
}

func (wal *DatabaseWAL) shouldSync() bool {
	if wal.node.IsReplica() {
		return false
	}

	if wal.checkpointing {
		return false
	}

	if time.Since(wal.createdAt) < DatabaseWALSyncInterval {
		return false
	}

	if time.Since(wal.lastSyncTime) < DatabaseWALSyncInterval {
		return false
	}

	return true
}

func (wal *DatabaseWAL) Size() (int64, error) {
	file, err := wal.File()

	if err != nil {
		log.Println(err)

		return 0, err
	}

	info, err := file.Stat()

	if err != nil {
		log.Println(err)
		return 0, err
	}

	size := info.Size()

	wal.lastKnownSize = size

	return size, nil
}

func (wal *DatabaseWAL) Sync() error {
	if wal.node.IsReplica() {
		return errors.New("cannot sync WAL file on replica node")
	}

	wal.syncMutex.Lock()
	defer wal.syncMutex.Unlock()

	wal.mutex.Lock()
	defer wal.mutex.Unlock()

	file, err := wal.File()

	if err != nil {
		log.Println(err)
		return err
	}

	return file.Sync()
}

func (wal *DatabaseWAL) Timestamp() int64 {
	return wal.timestamp
}

// This operation is a no-op. WAL version data is immutable.
func (wal *DatabaseWAL) Truncate(size int64) error {
	wal.mutex.Lock()
	defer wal.mutex.Unlock()

	if wal.node.IsReplica() {
		return errors.New("cannot truncate WAL file on replica node")
	}

	return nil
}

func (wal *DatabaseWAL) WriteAt(p []byte, off int64) (n int, err error) {
	if wal.node.IsReplica() {
		return 0, errors.New("cannot write to WAL file on replica node")
	}

	wal.mutex.Lock()
	defer wal.mutex.Unlock()

	wal.lastWriteTime = time.Now()

	cacheKey := fmt.Sprintf("%d", off)

	err = wal.cache.Put(cacheKey, slices.Clone(p))

	file, err := wal.File()

	if err != nil {
		log.Println(err)
		return 0, err
	}

	n, err = file.WriteAt(p, off)

	if wal.shouldSync() {
		wal.performAsynchronousSync()
	}

	wal.lastWriteTime = time.Now()

	return n, err
}
