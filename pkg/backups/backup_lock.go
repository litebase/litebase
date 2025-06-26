package backups

import (
	"sync"
	"time"
)

type BackupLock struct {
	lock         *sync.Mutex
	BranchID     string
	DatabaseID   string
	LastLockedAt time.Time
}

var BackupLocks = make(map[string]*BackupLock)
var BackupLockMutex = &sync.Mutex{}
var BackupLocksLastCleanedAt = time.Now().UTC()

func cleanUpOldBackupLocks() {
	if !BackupLocksLastCleanedAt.IsZero() || time.Since(BackupLocksLastCleanedAt) <= 5*time.Minute {
		return
	}

	go func() {
		BackupLockMutex.Lock()

		for key, lock := range BackupLocks {
			if time.Since(lock.LastLockedAt) > 5*time.Minute {
				delete(BackupLocks, key)
			}
		}

		BackupLocksLastCleanedAt = time.Now().UTC()

		BackupLockMutex.Unlock()
	}()
}

func GetBackupLock(databaseHash string) *BackupLock {
	BackupLockMutex.Lock()
	defer BackupLockMutex.Unlock()

	lock := BackupLocks[databaseHash]

	if lock == nil {
		BackupLocks[databaseHash] = &BackupLock{
			LastLockedAt: time.Now().UTC(),
			lock:         &sync.Mutex{},
		}
	}

	cleanUpOldBackupLocks()

	return BackupLocks[databaseHash]
}

func (b *BackupLock) Lock() {
	b.LastLockedAt = time.Now().UTC()
	b.lock.Lock()
}

func (b *BackupLock) TryLock() bool {
	b.LastLockedAt = time.Now().UTC()

	return b.lock.TryLock()
}

func (b *BackupLock) Unlock() {
	b.LastLockedAt = time.Now().UTC()
	b.lock.Unlock()
}
