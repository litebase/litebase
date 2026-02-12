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
	"github.com/litebase/litebase/pkg/cluster"
	"github.com/litebase/litebase/pkg/config"
	"github.com/litebase/litebase/pkg/file"
	"github.com/litebase/litebase/pkg/memory"
	"github.com/litebase/litebase/pkg/storage"
)

const (
	// WAL cache configuration
	WALCacheCapacity    = 32000 // Number of pages to cache (10000 pages × 4KB = 40MB per WAL)
	WALCacheDefaultSize = 4096  // 4KB per page
	// Read-ahead configuration for sequential scans
	WALReadAheadPages = 64 // Prefetch 64 pages (256KB) ahead for sequential reads
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
	BranchID                string
	cache                   *memory.ManagedLRUCache
	cacheKeyBuffer          []byte
	memoryManager           *memory.Manager
	createdAt               time.Time
	DatabaseID              string
	checkpointedAt          time.Time
	checkpointing           bool
	file                    internalStorage.File
	fileSystem              *storage.FileSystem
	hash                    string
	lastKnownSize           int64
	lastReadOffset          int64 // Track last read offset for sequential detection
	lastSyncTime            time.Time
	lastWriteTime           time.Time
	mutex                   *sync.RWMutex
	node                    *cluster.Node
	Path                    string
	prefetchInFlight        sync.Map // Track in-flight prefetch operations to avoid duplicates
	syncMutex               *sync.Mutex
	timestamp               int64
	walManager              *DatabaseWALManager
	inTransaction           bool
	transactionConnectionId string
	txnBuffer               *TransactionBuffer // Transaction buffer (protected by SQLite write lock)
	txnBufferAllocTried     bool
}

// walCacheKey is used as a cache key to avoid string allocations
type walCacheKey struct {
	Timestamp int64
	Offset    int64
}

func NewDatabaseWAL(
	node *cluster.Node,
	connectionManager *ConnectionManager,
	memoryManager *memory.Manager,
	databaseId string,
	branchId string,
	fileSystem *storage.FileSystem,
	walManager *DatabaseWALManager,
	timestamp int64,
) *DatabaseWAL {

	return &DatabaseWAL{
		BranchID: branchId,
		cache: memory.NewManagedLRUCache(memory.ManagedLRUCacheConfig{
			Capacity:    WALCacheCapacity,
			Manager:     memoryManager,
			DefaultSize: WALCacheDefaultSize,
			Owner:       fmt.Sprintf("wal-cache-%s-%s-%d", databaseId, branchId, timestamp),
		}),
		cacheKeyBuffer:      make([]byte, 0, 64),
		createdAt:           time.Now().UTC(),
		DatabaseID:          databaseId,
		fileSystem:          fileSystem,
		lastKnownSize:       -1,
		lastSyncTime:        time.Time{},
		memoryManager:       memoryManager,
		mutex:               &sync.RWMutex{},
		node:                node,
		Path:                fmt.Sprintf("%slogs/wal/WAL_%d", file.GetDatabaseFileBaseDir(databaseId, branchId), timestamp),
		syncMutex:           &sync.Mutex{},
		timestamp:           timestamp,
		txnBuffer:           nil,
		txnBufferAllocTried: false,
		walManager:          walManager,
	}
}

// Begin starts a new transaction for the given connection. It ensures that only
// one transaction can be active at a time for this WAL.
func (wal *DatabaseWAL) Begin(connectionID string) error {
	wal.mutex.Lock()
	defer wal.mutex.Unlock()

	if wal.inTransaction {
		return errors.New("transaction already in progress")
	}

	wal.inTransaction = true
	wal.transactionConnectionId = connectionID

	// Ensure we have a transaction buffer for this WAL. Allocate lazily
	// so the system doesn't pre-allocate buffers for WALs that are not
	// actively used. Only attempt allocation once per WAL to avoid
	// repeated allocation churn under memory pressure.
	if wal.txnBuffer == nil && !wal.txnBufferAllocTried {
		wal.txnBufferAllocTried = true

		if wal.memoryManager != nil {
			buf, err := NewTransactionBuffer(wal.memoryManager, wal.timestamp)

			if err != nil {
				slog.Debug("failed to allocate txn buffer for WAL", "timestamp", wal.timestamp, "error", err)
			} else {
				wal.txnBuffer = buf
			}
		}
	}

	return nil
}

func (wal *DatabaseWAL) Checkpointing() bool {
	return wal.checkpointing
}

func (wal *DatabaseWAL) Close() error {
	if wal.file != nil {
		err := wal.file.Close()

		if err != nil {
			slog.Error("failed to close WAL file", "error", err)
		}

		wal.file = nil
	}

	if wal.txnBuffer != nil {
		wal.txnBuffer.Release(wal.memoryManager)
		wal.txnBuffer = nil
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

	// Release any allocated transaction buffer lease so memory is returned.
	if wal.txnBuffer != nil {
		if err := wal.txnBuffer.Release(wal.memoryManager); err != nil {
			slog.Error("failed to release txn buffer during Delete", "error", err)
		}
		wal.txnBuffer = nil
	}

	err = wal.fileSystem.Remove(wal.Path)

	if err != nil {
		log.Println(err)
		return err
	}

	return nil
}

// End ends the current transaction, flushing any buffered writes to the WAL file.
func (wal *DatabaseWAL) End(connectionID string) error {
	wal.mutex.Lock()

	if !wal.inTransaction {
		wal.mutex.Unlock()
		return nil // Transaction already ended - this is okay
	}

	if wal.transactionConnectionId != connectionID {
		wal.mutex.Unlock()
		return errors.New("connection does not own the active transaction")
	}

	// Snapshot current buffer; release wal mutex before flushing to avoid
	// lock-order inversions with checkpointing/compaction code paths.
	buf := wal.txnBuffer
	wal.mutex.Unlock()

	// Flush buffer outside wal mutex to avoid lock-order inversions
	if buf != nil {
		writes := buf.GetWrites()

		if len(writes) > 0 {
			if err := wal.FlushBuffer(); err != nil {
				// Log flush errors but allow transaction end to proceed so caller can handle higher-level errors.
				slog.Error("Failed to flush transaction buffer on End", "error", err)
			}
		}
	}

	// Re-acquire wal mutex to update transaction state
	wal.mutex.Lock()

	// Re-check that transaction is still active (could have been ended by another thread)
	if !wal.inTransaction || wal.transactionConnectionId != connectionID {
		wal.mutex.Unlock()
		return nil // Transaction already ended, this is okay
	}

	wal.inTransaction = false
	wal.transactionConnectionId = ""
	wal.mutex.Unlock()

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

func (wal *DatabaseWAL) FlushBuffer() error {
	// Grab a snapshot of the buffer without holding wal mutex to avoid
	// lock-order inversions (wal.mutex vs checkpoint mutex) that can
	// deadlock under contention. The TransactionBuffer has its own
	// synchronization for reads.
	if wal.txnBuffer == nil {
		return nil
	}

	writes := wal.txnBuffer.GetWrites()

	if len(writes) == 0 {
		return nil
	}

	file, err := wal.File()

	if err != nil {
		return fmt.Errorf("failed to get WAL file for flush: %w", err)
	}

	slog.Debug("Flushing transaction buffer", "writes", len(writes), "timestamp", wal.timestamp)

	for _, w := range writes {
		if _, err := file.WriteAt(w.data, w.offset); err != nil {
			slog.Error("Failed to write buffered data during flush", "error", err, "offset", w.offset)
			return fmt.Errorf("failed to flush transaction buffer at offset %d: %w", w.offset, err)
		}
	}

	// Batch-update cache and metadata under wal mutex to minimize lock hold time
	wal.mutex.Lock()
	for _, w := range writes {
		cacheKey := wal.getCacheKey(w.offset)

		if cacheErr := wal.cache.Put(cacheKey, w.data); cacheErr != nil {
			slog.Error("Error caching WAL data during flush", "error", cacheErr)
		}
	}

	// Update last write time
	wal.lastWriteTime = time.Now().UTC()
	wal.mutex.Unlock()

	// Sync once at the end if needed
	if wal.shouldSync() {
		if err := file.Sync(); err != nil {
			slog.Error("Failed to sync WAL after transaction flush", "error", err)
		}
	}

	// Update last write time (protect write with mutex)
	wal.mutex.Lock()
	wal.lastWriteTime = time.Now().UTC()
	wal.mutex.Unlock()

	// Clear buffer after successful flush
	wal.txnBuffer.Clear()

	slog.Debug("Successfully flushed transaction buffer", "writes", len(writes), "timestamp", wal.timestamp)

	return nil
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

func (wal *DatabaseWAL) ReadAt(connectionID string, p []byte, off int64) (n int, err error) {
	// If this connection owns the transaction buffer, read from it first
	wal.mutex.RLock()
	inTxn := wal.inTransaction && wal.txnBuffer != nil && wal.transactionConnectionId == connectionID
	buf := wal.txnBuffer
	wal.mutex.RUnlock()

	if inTxn && buf != nil {
		if n, err := buf.ReadAt(p, off); err == nil {
			return n, nil
		}
		// Fall through to read from file if not present in buffer
	}

	// Read from WAL file (committed writes)
	wal.mutex.Lock()
	defer wal.mutex.Unlock()

	// Check cache first
	cacheKey := wal.getCacheKey(off)

	if data, found := wal.cache.Get(cacheKey); found {
		if cachedData, ok := data.([]byte); ok && len(cachedData) >= len(p) {
			return copy(p, cachedData[:len(p)]), nil
		}
	}

	file, err := wal.File()

	if err != nil {
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

	// Update last read offset for sequential detection
	wal.lastReadOffset = off

	// Cache the read data
	if cacheErr := wal.cache.Put(cacheKey, p[:n]); cacheErr != nil {
		slog.Error("Error caching WAL data", "error", cacheErr)
	}

	return n, nil
}

// prefetchPages asynchronously prefetches pages into the cache for sequential scans.
// This reduces I/O latency by reading ahead of the current position.
// Note: Takes file handle as parameter to avoid lock acquisition.
func (wal *DatabaseWAL) prefetchPages(file internalStorage.File, startOffset int64, numPages int) {
	// Check if prefetch is already in flight for this offset range
	prefetchKey := fmt.Sprintf("%d-%d", startOffset, numPages)

	if _, inFlight := wal.prefetchInFlight.LoadOrStore(prefetchKey, true); inFlight {
		return // Prefetch already in progress
	}

	defer wal.prefetchInFlight.Delete(prefetchKey)

	// Prefetch pages in a single large read to reduce syscalls
	pageSize := int64(WALCacheDefaultSize)
	batchSize := int64(numPages) * pageSize
	buf := make([]byte, batchSize)

	// Read batch of pages without holding any locks
	n, err := file.ReadAt(buf, startOffset)

	if err != nil && n == 0 {
		return // End of file or read error
	}

	// Cache each page from the batch
	// Only acquire lock when updating cache
	for i := int64(0); i < int64(n)/pageSize; i++ {
		pageOffset := startOffset + (i * pageSize)
		pageStart := i * pageSize
		pageEnd := pageStart + pageSize

		if pageEnd > int64(n) {
			pageEnd = int64(n)
		}

		pageData := buf[pageStart:pageEnd]
		cacheKey := wal.getCacheKey(pageOffset)

		// Check cache without lock first
		wal.mutex.RLock()
		_, found := wal.cache.Get(cacheKey)
		wal.mutex.RUnlock()

		if !found {
			// Acquire write lock only to insert
			wal.mutex.Lock()
			// Double-check after acquiring lock
			if _, stillNotFound := wal.cache.Get(cacheKey); stillNotFound {
				if err := wal.cache.Put(cacheKey, pageData); err != nil {
					wal.mutex.Unlock()
					// Cache full, stop prefetching
					break
				}
			}
			wal.mutex.Unlock()
		}
	}
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

// WriteAt writes to the WAL file, using transaction buffer if this connection owns it.
func (wal *DatabaseWAL) WriteAt(connectionID string, p []byte, off int64) (n int, err error) {
	// Try to buffer when this connection owns the txn buffer.
	wal.mutex.RLock()
	inTxn := wal.inTransaction && wal.txnBuffer != nil && wal.transactionConnectionId == connectionID
	buf := wal.txnBuffer
	wal.mutex.RUnlock()

	if inTxn && buf != nil {
		if n, err := buf.WriteAt(p, off); err != nil {
			if err == ErrBufferCapacity {
				// Buffer is full — flush the buffer to WAL and then retry buffering.
				if ferr := wal.FlushBuffer(); ferr != nil {
					// If flush fails, fall back to direct write to avoid data loss.
					wal.mutex.Lock()
					defer wal.mutex.Unlock()
					return wal.writeAtDirect(p, off)
				}

				// Retry buffering after successful flush
				return buf.WriteAt(p, off)
			}

			return 0, err
		} else {
			return n, nil
		}
	}

	// Not in transaction or different connection — write directly
	wal.mutex.Lock()
	defer wal.mutex.Unlock()

	return wal.writeAtDirect(p, off)
}

// writeAtDirect writes directly to the WAL file without buffering.
// This is used by both direct writes and when flushing transaction buffers.
func (wal *DatabaseWAL) writeAtDirect(p []byte, off int64) (n int, err error) {

	wal.lastWriteTime = time.Now().UTC()

	file, err := wal.File()

	if err != nil {
		slog.Error("Error getting WAL file", "error", err)
		return 0, err
	}

	n, err = file.WriteAt(p, off)

	if err != nil {
		return n, err
	}

	// Update cache after successful write
	cacheKey := wal.getCacheKey(off)

	if cacheErr := wal.cache.Put(cacheKey, p[:n]); cacheErr != nil {
		slog.Error("Error caching WAL data", "error", cacheErr)
	}

	if wal.shouldSync() {
		wal.performAsynchronousSync()
	}

	wal.lastWriteTime = time.Now().UTC()

	return n, err
}
