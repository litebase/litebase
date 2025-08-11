package backups

import (
	"sync"
	"time"
)

type BackupLock struct {
	lock         *sync.Mutex
	BranchID     string
	DatabaseID   string
	lastLockedAt time.Time
	mu           sync.RWMutex // protects lastLockedAt
}

var BackupLocks = make(map[string]*BackupLock)
var BackupLockMutex = &sync.Mutex{}
var BackupLocksLastCleanedAt = time.Now().UTC()
var cleanupInProgress = false

func cleanUpOldBackupLocks() {
	BackupLockMutex.Lock()

	// Check if enough time has passed and no cleanup is already in progress
	if time.Since(BackupLocksLastCleanedAt) <= 5*time.Minute || cleanupInProgress {
		BackupLockMutex.Unlock()
		return
	}

	// Mark cleanup as in progress
	cleanupInProgress = true
	BackupLockMutex.Unlock()

	// Run cleanup in goroutine
	go func() {
		BackupLockMutex.Lock()
		defer BackupLockMutex.Unlock()

		now := time.Now().UTC()
		for key, lock := range BackupLocks {
			lock.mu.RLock()
			lastLocked := lock.lastLockedAt
			lock.mu.RUnlock()

			if now.Sub(lastLocked) > 5*time.Minute {
				delete(BackupLocks, key)
			}
		}

		BackupLocksLastCleanedAt = now
		cleanupInProgress = false
	}()
}

func GetBackupLock(databaseHash string) *BackupLock {
	BackupLockMutex.Lock()

	lock := BackupLocks[databaseHash]

	if lock == nil {
		BackupLocks[databaseHash] = &BackupLock{
			lastLockedAt: time.Now().UTC(),
			lock:         &sync.Mutex{},
		}
	}

	result := BackupLocks[databaseHash]
	BackupLockMutex.Unlock()

	// Call cleanup after releasing the lock
	cleanUpOldBackupLocks()

	return result
}

// Lock locks the backup lock.
func (bl *BackupLock) Lock() {
	bl.lock.Lock()
	bl.mu.Lock()
	bl.lastLockedAt = time.Now()
	bl.mu.Unlock()
}

func (b *BackupLock) TryLock() bool {
	locked := b.lock.TryLock()

	if !locked {
		return false
	}

	b.mu.Lock()
	b.lastLockedAt = time.Now().UTC()
	b.mu.Unlock()

	return true
}

func (b *BackupLock) Unlock() {
	b.lock.Unlock()
	b.mu.Lock()
	b.lastLockedAt = time.Now().UTC()
	b.mu.Unlock()
}

func (b *BackupLock) GetLastLockedAt() time.Time {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.lastLockedAt
}
