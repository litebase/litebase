package database

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	internalStorage "github.com/litebase/litebase/internal/storage"
	"github.com/litebase/litebase/pkg/cache"
	"github.com/litebase/litebase/pkg/cluster"
	"github.com/litebase/litebase/pkg/file"
	"github.com/litebase/litebase/pkg/storage"
)

var (
	DatabaseWALSyncInterval = 100 * time.Millisecond
)

// A Write Ahead Log provides crash recovery for a database. In this application
// the WAL also servers as an immediate buffer of changes to be written to the
// the database. These data changes are synced quite frequently as the WAL is
// checkpointed to durable storage.
//
// This WAL uses a LFU cache to store recently read/written data pages to avoid
// excessive file i/o. Note to determine the max size of the cache, we must
// consider the number of cached items which may be 24 bytes for a SQLITE WAL
// Frame header and 4KB for the contents of the page.
type DatabaseWAL struct {
	BranchID       string
	cache          *cache.LFUCache
	cacheKeyBuffer []byte
	createdAt      time.Time
	DatabaseID     string
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
		BranchID:       branchId,
		cache:          cache.NewLFUCache(16000), // ~33MB
		cacheKeyBuffer: make([]byte, 0, 64),
		createdAt:      time.Now().UTC(),
		DatabaseID:     databaseId,
		fileSystem:     fileSystem,
		lastKnownSize:  -1,
		lastSyncTime:   time.Time{},
		mutex:          &sync.RWMutex{},
		node:           node,
		Path:           fmt.Sprintf("%slogs/wal/WAL_%d", file.GetDatabaseFileBaseDir(databaseId, branchId), timestamp),
		syncMutex:      &sync.Mutex{},
		timestamp:      timestamp,
		walManager:     walManager,
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
	file, err := wal.fileSystem.OpenFile(
		wal.Path,
		os.O_CREATE|os.O_RDWR,
		0600,
	)

	if err != nil {
		if os.IsNotExist(err) {
			err = wal.fileSystem.MkdirAll(filepath.Dir(wal.Path), 0750)

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

func (wal *DatabaseWAL) getCacheKey(offset int64) string {
	wal.cacheKeyBuffer = wal.cacheKeyBuffer[:0]
	wal.cacheKeyBuffer = strconv.AppendInt(wal.cacheKeyBuffer, offset, 10)

	return string(wal.cacheKeyBuffer)
}

func (wal *DatabaseWAL) Hash() string {
	if wal.hash != "" {
		return wal.hash
	}

	checksum := sha256.Sum256(fmt.Appendf(nil, "%s:%s:%d", wal.DatabaseID, wal.BranchID, wal.Timestamp()))
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
	wal.checkpointedAt = time.Now().UTC()
}

func (wal *DatabaseWAL) performAsynchronousSync() {
	go func() {
		if !wal.syncMutex.TryLock() {
			return
		}

		defer wal.syncMutex.Unlock()

		wal.mutex.Lock()

		defer func() {
			wal.mutex.Unlock()
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

		wal.lastSyncTime = time.Now().UTC()
	}()
}

func (wal *DatabaseWAL) ReadAt(p []byte, off int64) (n int, err error) {
	wal.mutex.RLock()
	defer wal.mutex.RUnlock()

	cacheKey := wal.getCacheKey(off)

	if data, found := wal.cache.Get(cacheKey); found && len(data.([]byte)) == len(p) {
		if cachedData, ok := data.([]byte); ok && len(cachedData) >= len(p) {
			return copy(p, cachedData[:len(p)]), nil
		}
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

	cachedData := make([]byte, n)
	copy(cachedData, p[:n])

	// Cache the read data
	err = wal.cache.Put(cacheKey, cachedData)

	if err != nil {
		slog.Error("Error caching WAL data", "error", err)
	}

	return n, nil
}

func (wal *DatabaseWAL) RequiresCheckpoint() bool {
	if wal.lastKnownSize < 0 {
		_, err := wal.Size()

		if err != nil {
			slog.Error("Error getting WAL size", "error", err)

			return false
		}
	}

	return wal.checkpointedAt.IsZero() && (wal.lastKnownSize > 0 || !wal.lastWriteTime.IsZero())
}

func (wal *DatabaseWAL) SetCheckpointing(checkpointing bool) error {
	if wal.node.IsReplica() {
		return errors.New("cannot set checkpointing on replica node")
	}

	wal.mutex.Lock()
	defer wal.mutex.Unlock()
	wal.checkpointing = checkpointing

	return nil
}

// IsCheckpointing returns whether this WAL is currently being checkpointed
func (wal *DatabaseWAL) IsCheckpointing() bool {
	wal.mutex.RLock()
	defer wal.mutex.RUnlock()
	return wal.checkpointing
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
	wal.mutex.Lock()
	defer wal.mutex.Unlock()

	wal.lastWriteTime = time.Now().UTC()

	cacheKey := wal.getCacheKey(off)

	cachedData := make([]byte, n)
	copy(cachedData, p[:n])

	err = wal.cache.Put(cacheKey, cachedData)

	file, err := wal.File()

	if err != nil {
		log.Println(err)
		return 0, err
	}

	n, err = file.WriteAt(p, off)

	if wal.shouldSync() {
		wal.performAsynchronousSync()
	}

	wal.lastWriteTime = time.Now().UTC()

	return n, err
}
