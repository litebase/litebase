package storage

import (
	"testing"
)

func TestNewFileSystemLock(t *testing.T) {
	fsl := NewFileSystemLock()

	if fsl == nil {
		t.Fatal("Expected NewFileSystemLock to return a non-nil value")
	}

	if fsl.lock == nil {
		t.Fatal("Expected lock map to be initialized")
	}
}

func TestFileSystemLock_AcquireAccessLocks(t *testing.T) {
	fsl := NewFileSystemLock()
	path := "/test/lock/path"

	locks := fsl.AcquireAccessLocks(path)

	// Ensure that AcquireAccessLocks returns a non-empty slice
	if len(locks) == 0 {
		t.Fatal("Expected AcquireAccessLocks to return a non-empty slice")
	}

	// Ensure that there are 4 locks for the path and its parents
	if len(locks) != 4 {
		t.Errorf("Expected 4 locks for the path and its parents, got %d", len(locks))
	}

	for _, lock := range locks {
		if lock.count != 1 {
			t.Errorf("Expected lock count to be 1, got %d", lock.count)
		}

		lock.mutex.RUnlock()
	}
}

func TestFileSystemLock_AcquireDeleteLocks(t *testing.T) {
	fsl := NewFileSystemLock()
	path := "/test/path/to/delete"

	locks := fsl.AcquireDeleteLocks(path)

	if len(locks) == 0 {
		t.Fatal("Expected AcquireDeleteLocks to return a non-empty slice")
	}

	for _, lock := range locks {
		if lock.count != 1 {
			t.Errorf("Expected lock count to be 1, got %d", lock.count)
		}

		lock.mutex.Unlock()
	}

}

func TestFileSystemLock_AcquirePathReadLock(t *testing.T) {
	fsl := NewFileSystemLock()
	path := "/test/path/to/read"

	lock := fsl.AcquirePathReadLock(path)

	if lock == nil {
		t.Fatal("Expected AcquirePathReadLock to return a non-nil lock")
	}

	if lock.count != 1 {
		t.Errorf("Expected lock count to be 1, got %d", lock.count)
	}

	lock.mutex.RUnlock()
}

func TestFileSystemLock_AcquirePathWriteLock(t *testing.T) {
	fsl := NewFileSystemLock()
	path := "/test/path/to/write"

	lock := fsl.AcquirePathWriteLock(path)

	if lock == nil {
		t.Fatal("Expected AcquirePathWriteLock to return a non-nil lock")
	}

	if lock.count != 1 {
		t.Errorf("Expected lock count to be 1, got %d", lock.count)
	}

	lock.mutex.Unlock()
}

func TestFileSystemLock_DeleteLockIfUnused(t *testing.T) {
	fsl := NewFileSystemLock()
	path := "/test/path/to/delete"

	// Acquire a lock
	lock := fsl.AcquirePathWriteLock(path)

	// Release the lock
	lock.mutex.Unlock()
	lock.count--

	// Attempt to delete the lock
	fsl.DeleteLockIfUnused(path)

	// Check if the lock was deleted
	if _, ok := fsl.lock.Load(path); ok {
		t.Fatal("Expected DeleteLockIfUnused to remove the lock entry")
	}
}

func TestFileSystemLock_GetLockEntry(t *testing.T) {
	fsl := NewFileSystemLock()
	path := "/test/path/to/get"

	// Get lock entry for the path
	lockEntry := fsl.GetLockEntry(path)

	if lockEntry == nil {
		t.Fatal("Expected GetLockEntry to return a non-nil lock entry")
	}

	if lockEntry.path != path {
		t.Errorf("Expected lock entry path to be %s, got %s", path, lockEntry.path)
	}
}

func TestFileSystemLock_ReleaseAccessLocks(t *testing.T) {
	fsl := NewFileSystemLock()
	path := "/test/path/to/release/access"

	locks := fsl.AcquireAccessLocks(path)

	if len(locks) == 0 {
		t.Fatal("Expected AcquireAccessLocks to return a non-empty slice")
	}

	fsl.ReleaseAccessLocks(locks)

	for _, lock := range locks {
		if lock.count != 0 {
			t.Errorf("Expected lock count to be 0 after release, got %d", lock.count)
		}
	}
}

func TestFileSystemLock_ReleaseDeleteLocks(t *testing.T) {
	fsl := NewFileSystemLock()
	path := "/test/path/to/release/delete"

	locks := fsl.AcquireDeleteLocks(path)

	if len(locks) == 0 {
		t.Fatal("Expected AcquireDeleteLocks to return a non-empty slice")
	}

	fsl.ReleaseDeleteLocks(locks)

	for _, lock := range locks {
		if lock.count != 0 {
			t.Errorf("Expected lock count to be 0 after release, got %d", lock.count)
		}
	}
}

func TestFileSystemLock_ReleasePathReadLock(t *testing.T) {
	fsl := NewFileSystemLock()
	path := "/test/path/to/release/read"

	lock := fsl.AcquirePathReadLock(path)

	if lock == nil {
		t.Fatal("Expected AcquirePathReadLock to return a non-nil lock")
	}

	fsl.ReleasePathReadLock(lock)

	if lock.count != 0 {
		t.Errorf("Expected lock count to be 0 after release, got %d", lock.count)
	}
}

func TestFileSystemLock_ReleasePathWriteLock(t *testing.T) {
	fsl := NewFileSystemLock()
	path := "/test/path/to/release/write"

	lock := fsl.AcquirePathWriteLock(path)

	if lock == nil {
		t.Fatal("Expected AcquirePathWriteLock to return a non-nil lock")
	}

	fsl.ReleasePathWriteLock(lock)

	if lock.count != 0 {
		t.Errorf("Expected lock count to be 0 after release, got %d", lock.count)
	}
}
