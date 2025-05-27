package storage

import (
	"path/filepath"
	"strings"
	"sync"
)

type FileSystemLockEntry struct {
	count int
	mutex sync.RWMutex
	path  string
}

type FileSystemLock struct {
	lock *sync.Map
}

// Create a new FileSystemLock instance.
func NewFileSystemLock() *FileSystemLock {
	return &FileSystemLock{
		lock: &sync.Map{},
	}
}

// Acquire a access locks for the specified path.
func (fsl *FileSystemLock) AcquireAccessLocks(path string) []*FileSystemLockEntry {
	parts := strings.Split(path, "/")

	var acquired []*FileSystemLockEntry
	currentPath := ""

	for _, part := range parts {
		if part != "" {
			currentPath += string(filepath.Separator)
		}

		currentPath += part
		entry := fsl.GetLockEntry(currentPath)
		entry.mutex.RLock()
		entry.count++
		acquired = append(acquired, entry)
	}

	return acquired
}

// Acquire a locks for deleting a path and all its parent directories.
func (fsl *FileSystemLock) AcquireDeleteLocks(path string) []*FileSystemLockEntry {
	parts := strings.Split(strings.TrimRight(path, "/"), "/")
	var acquired []*FileSystemLockEntry
	currentPath := ""

	for _, part := range parts {
		if part != "" {
			currentPath += string(filepath.Separator)
		}

		currentPath += part
		entry := fsl.GetLockEntry(currentPath)
		entry.mutex.Lock()
		entry.count++
		acquired = append(acquired, entry)
	}

	return acquired
}

// Acquire a read lock for the specified path.
func (fsl *FileSystemLock) AcquirePathReadLock(path string) *FileSystemLockEntry {
	lock := fsl.GetLockEntry(path)

	lock.mutex.RLock()
	lock.count++

	return lock
}

// Acquire a write lock for the specified path.
func (fsl *FileSystemLock) AcquirePathWriteLock(path string) *FileSystemLockEntry {
	lock := fsl.GetLockEntry(path)

	lock.mutex.Lock()
	lock.count++

	return lock
}

// Remove a lock entry if it is not in use anymore.
func (fsl *FileSystemLock) DeleteLockIfUnused(path string) {
	if entry, ok := fsl.lock.Load(path); ok {
		lock := entry.(*FileSystemLockEntry)

		if lock.count == 0 {
			fsl.lock.Delete(path)
		}
	}
}

// Get the lock entry for the specified path, creating it if it does not exist.
func (fsl *FileSystemLock) GetLockEntry(path string) *FileSystemLockEntry {
	entry, _ := fsl.lock.LoadOrStore(path, &FileSystemLockEntry{
		path: path,
	})

	return entry.(*FileSystemLockEntry)
}

// Release the access locks acquired for the specified path.
func (fsl *FileSystemLock) ReleaseAccessLocks(locks []*FileSystemLockEntry) {
	for _, lock := range locks {
		lock.mutex.RUnlock()
		lock.count--

		go fsl.DeleteLockIfUnused(lock.path)
	}
}

// Release the delete locks acquired for the specified path and its parents.
func (fsl *FileSystemLock) ReleaseDeleteLocks(locks []*FileSystemLockEntry) {
	for _, lock := range locks {
		lock.mutex.Unlock()
		lock.count--

		go fsl.DeleteLockIfUnused(lock.path)
	}
}

// Release the read or write lock for the specified path.
func (fsl *FileSystemLock) ReleasePathReadLock(lock *FileSystemLockEntry) {
	lock.mutex.RUnlock()
	lock.count--
	go fsl.DeleteLockIfUnused(lock.path)
}

// Release the write lock for the specified path.
func (fsl *FileSystemLock) ReleasePathWriteLock(lock *FileSystemLockEntry) {
	lock.mutex.Unlock()
	lock.count--
	go fsl.DeleteLockIfUnused(lock.path)
}
