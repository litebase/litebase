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
	lock *sync.Map // map[string]*FileSystemLockEntry
}

func NewFileSystemLock() *FileSystemLock {
	return &FileSystemLock{
		lock: &sync.Map{},
	}
}

func (fsl *FileSystemLock) deleteLockIfUnused(path string) {
	if entry, ok := fsl.lock.Load(path); ok {
		lock := entry.(*FileSystemLockEntry)

		if lock.count == 0 {
			fsl.lock.Delete(path)
		}
	}
}

func (fsl *FileSystemLock) acquireAccessLocks(path string) []*FileSystemLockEntry {
	parts := strings.Split(path, "/")

	var acquired []*FileSystemLockEntry
	currentPath := ""

	for _, part := range parts {
		if part != "" {
			currentPath += string(filepath.Separator)
		}

		currentPath += part
		entry := fsl.getLockEntry(currentPath)
		entry.mutex.RLock()
		entry.count++
		acquired = append(acquired, entry)
	}

	return acquired
}

func (fsl *FileSystemLock) acquireDeleteLocks(path string) []*FileSystemLockEntry {
	parts := strings.Split(path, "/")
	var acquired []*FileSystemLockEntry
	currentPath := ""

	for _, part := range parts {
		if part != "" {
			currentPath += string(filepath.Separator)
		}

		currentPath += part
		entry := fsl.getLockEntry(currentPath)
		entry.mutex.Lock()
		entry.count++
		acquired = append(acquired, entry)
	}

	return acquired
}

func (fsl *FileSystemLock) acquirePathReadLock(path string) *FileSystemLockEntry {
	lock := fsl.getLockEntry(path)

	lock.mutex.RLock()
	lock.count++

	return lock
}

func (fsl *FileSystemLock) acquirePathWriteLock(path string) *FileSystemLockEntry {
	lock := fsl.getLockEntry(path)

	lock.mutex.Lock()
	lock.count++

	return lock
}

func (fsl *FileSystemLock) getLockEntry(path string) *FileSystemLockEntry {
	entry, _ := fsl.lock.LoadOrStore(path, &FileSystemLockEntry{
		path: path,
	})

	return entry.(*FileSystemLockEntry)
}

func (fsl *FileSystemLock) releaseAccessLocks(locks []*FileSystemLockEntry) {
	for _, lock := range locks {
		lock.mutex.RUnlock()
		lock.count--

		go fsl.deleteLockIfUnused(lock.path)
	}
}

func (fsl *FileSystemLock) releaseDeleteLocks(locks []*FileSystemLockEntry) {
	for _, lock := range locks {
		lock.mutex.Unlock()
		lock.count--

		go fsl.deleteLockIfUnused(lock.path)
	}
}

func (fsl *FileSystemLock) releasePathReadLock(lock *FileSystemLockEntry) {
	lock.mutex.RUnlock()
	lock.count--
	go fsl.deleteLockIfUnused(lock.path)
}

func (fsl *FileSystemLock) releasePathWriteLock(lock *FileSystemLockEntry) {
	lock.mutex.Unlock()
	lock.count--
	go fsl.deleteLockIfUnused(lock.path)
}
