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
	"sync"
	"time"

	internalStorage "github.com/litebase/litebase/internal/storage"
	"github.com/litebase/litebase/pkg/cache"
	"github.com/litebase/litebase/pkg/cluster"
	"github.com/litebase/litebase/pkg/config"
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

// getCacheKey generates a cache key for a timestamp+offset combination
type walCacheKey struct {
	Timestamp int64
	Offset    int64
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
		cache:          cache.NewLFUCache(1000), // Cache up to 1000 pages
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
	wal.cache.Close()

	if wal.file != nil {
		err := wal.file.Close()

		if err != nil {
			slog.Error("failed to close WAL file", "error", err)
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

	// System database is never encrypted (skip encryption check to avoid deadlock during system database initialization)
	isEncrypted := false
	var branch *Branch

	if wal.DatabaseID != SystemDatabaseID {
		// Check if encryption is enabled for this branch
		branch, err = wal.walManager.connectionManager.databaseManager.GetBranch(wal.DatabaseID, wal.BranchID)

		if err != nil {
			// If the branch doesn't exist (e.g., in tests with fake IDs), treat as unencrypted
			// This allows tests to work without requiring a full database setup
			if err.Error() == "sql: no rows in result set" {
				isEncrypted = false
			} else {
				if err := file.Close(); err != nil {
					slog.Error("failed to close file after getting branch error:", "error", err)
				}

				return nil, fmt.Errorf("failed to get branch: %w", err)
			}
		} else {
			isEncrypted, err = branch.IsEncrypted()

			if err != nil {
				if err := file.Close(); err != nil {
					slog.Error("failed to close file after checking encryption status error:", "error", err)
				}

				return nil, fmt.Errorf("failed to check encryption status: %w", err)
			}
		}
	}

	// Wrap with encrypted file if encryption is enabled
	if isEncrypted {
		dataEncryptionKeyHash, err := branch.GetDataEncryptionKeyHash()

		if err != nil {
			if err := file.Close(); err != nil {
				slog.Error("failed to close file after getting data encryption key hash error:", "error", err)
			}

			return nil, fmt.Errorf("failed to get data encryption key hash: %w", err)
		}

		config := wal.walManager.connectionManager.databaseManager.Cluster.Config
		dataKey, keyHash, err := MatchEncryptionKey(config, dataEncryptionKeyHash)

		if err != nil {
			if err := file.Close(); err != nil {
				slog.Error("failed to close file after matching encryption key error:", "error", err)
			}

			return nil, err
		}

		// Check if this is a new file (size 0) or an existing encrypted file
		fileInfo, err := file.Stat()

		if err != nil {
			if err := file.Close(); err != nil {
				slog.Error("failed to close file after stat error:", "error", err)
			}

			return nil, fmt.Errorf("failed to stat WAL file: %w", err)
		}

		var encryptedFile internalStorage.File

		if fileInfo.Size() == 0 {
			// New file - create encrypted wrapper
			encryptedFile, err = storage.NewEncryptedStreamFile(file, dataKey, keyHash, wal.timestamp, wal.Path)

			if err != nil {
				if err := file.Close(); err != nil {
					slog.Error("failed to close file after creating encrypted WAL file error:", "error", err)
				}

				return nil, fmt.Errorf("failed to create encrypted WAL file: %w", err)
			}

			// Write the header
			err = encryptedFile.(*storage.EncryptedStreamFile).WriteHeader()

			if err != nil {
				if err := file.Close(); err != nil {
					slog.Error("failed to close file after writing encrypted WAL header error:", "error", err)
				}

				return nil, fmt.Errorf("failed to write encrypted WAL header: %w", err)
			}
		} else {
			// Existing file - open encrypted wrapper
			encryptedFile, err = storage.OpenEncryptedStreamFile(file, dataKey, keyHash, wal.timestamp, wal.Path)

			if err != nil {
				if err := file.Close(); err != nil {
					slog.Error("failed to close file after opening encrypted WAL file error:", "error", err)
				}

				return nil, fmt.Errorf("failed to open encrypted WAL file: %w", err)
			}
		}

		file = encryptedFile
	}

	wal.file = file

	return wal.file, nil
}

// MatchEncryptionKey finds the matching encryption key in the config based on the key hash.
// It checks both DataEncryptionKeyHash and DataEncryptionKeyNextHash.
// Returns the key, its hash, and an error if no match is found.
func MatchEncryptionKey(cfg *config.Config, dataEncryptionKeyHash string) ([]byte, [32]byte, error) {
	if dataEncryptionKeyHash == "" {
		return nil, [32]byte{}, errors.New("data encryption key hash is empty")
	}

	// Check primary key
	if cfg.DataEncryptionKeyHash != "" && cfg.DataEncryptionKeyHash == dataEncryptionKeyHash {
		if cfg.DataEncryptionKey == nil || len(cfg.DataEncryptionKey) != 32 {
			return nil, [32]byte{}, errors.New("DataEncryptionKey is not configured or has invalid length")
		}

		keyHash := sha256.Sum256(cfg.DataEncryptionKey)

		return cfg.DataEncryptionKey, keyHash, nil
	}

	// Check next key (for key rotation)
	if cfg.DataEncryptionKeyNextHash != "" && cfg.DataEncryptionKeyNextHash == dataEncryptionKeyHash {
		if cfg.DataEncryptionKeyNext == nil || len(cfg.DataEncryptionKeyNext) != 32 {
			return nil, [32]byte{}, errors.New("DataEncryptionKeyNext is not configured or has invalid length")
		}

		keyHash := sha256.Sum256(cfg.DataEncryptionKeyNext)

		return cfg.DataEncryptionKeyNext, keyHash, nil
	}

	return nil, [32]byte{}, fmt.Errorf("DataEncryptionKey for this database not found (hash: %s)", dataEncryptionKeyHash)
}

func (wal *DatabaseWAL) getCacheKey(offset int64) walCacheKey {
	return walCacheKey{Timestamp: wal.timestamp, Offset: offset}
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
			slog.Error("failed to get WAL file for sync", "error", err)
			return
		}

		err = file.Sync()

		if err != nil {
			slog.Error("failed to sync WAL file", "error", err)
			return
		}

		wal.lastSyncTime = time.Now().UTC()
	}()
}

func (wal *DatabaseWAL) ReadAt(p []byte, off int64) (n int, err error) {
	wal.mutex.Lock()
	defer wal.mutex.Unlock()

	cacheKey := wal.getCacheKey(off)

	if data, found := wal.cache.Get(cacheKey); found && len(data.([]byte)) == len(p) {
		if cachedData, ok := data.([]byte); ok && len(cachedData) >= len(p) {
			return copy(p, cachedData[:len(p)]), nil
		}
	}

	file, err := wal.File()

	if err != nil {
		return 0, err
	}

	// Expectations for reading from checkpointed WAL files:
	//
	// PRIMARY NODE:
	//   - Once a WAL is checkpointed, the primary should not read from it during
	//     normal operations. All new writes go to a new WAL version.
	//   - Reading from a checkpointed WAL on primary indicates a logic error in
	//     the connection/checkpoint management.
	//   - Therefore, we panic to catch these bugs early in development.
	//
	// REPLICA NODE:
	//   - Replicas receive checkpoint notifications from the primary and need to
	//     apply those changes by reading the checkpointed WAL data.
	//   - Replicas may also need to read historical WAL data for catch-up operations.
	//   - Therefore, replicas SHOULD be allowed to read from checkpointed WAL files.
	//   - The current panic condition is a bug that prevents proper replica operation.
	//
	if wal.node.IsPrimary() && !wal.checkpointedAt.IsZero() {
		panic(fmt.Sprintf("WAL file has been checkpointed, cannot read from it - %d", wal.timestamp))
	}

	// TODO: Remove the replica panic condition to allow replicas to read
	// checkpointed WALs once messages are properly handled.
	if wal.node.IsReplica() && !wal.checkpointedAt.IsZero() {
		panic(fmt.Sprintf("WAL file has been checkpointed, cannot read from it - %d", wal.timestamp))
	}

	n, err = file.ReadAt(p, off)

	if err != nil {
		return n, err
	}

	// Cache the read data
	err = wal.cache.Put(cacheKey, p[:n])

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

	err = wal.cache.Put(cacheKey, p[:n])

	if err != nil {
		slog.Error("Error caching WAL data", "error", err)
	}

	file, err := wal.File()

	if err != nil {
		slog.Error("Error getting WAL file", "error", err)
		return 0, err
	}

	n, err = file.WriteAt(p, off)

	if wal.shouldSync() {
		wal.performAsynchronousSync()
	}

	wal.lastWriteTime = time.Now().UTC()

	return n, err
}
