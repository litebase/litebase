package storage

import (
	"fmt"
	internalStorage "litebasedb/internal/storage"
	"log"
	"os"
	"sync"
)

type LocalDatabaseFileSystem struct {
	files      map[string]internalStorage.File
	filesystem *LocalFileSystemDriver
	mutex      *sync.RWMutex
	path       string
	pageSize   int64
}

func NewLocalDatabaseFileSystem(path, databaseUuid, branchUuid string, pageSize int64) *LocalDatabaseFileSystem {
	// Check if the the directory exists
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			// Create the directory
			if err := os.MkdirAll(path, 0755); err != nil {
				log.Fatalln("Error creating temp file system directory", err)
			}
		} else {
			log.Fatalln("Error checking temp file system directory", err)
		}
	}

	return &LocalDatabaseFileSystem{
		files:      make(map[string]internalStorage.File),
		filesystem: NewLocalFileSystemDriver(),
		mutex:      &sync.RWMutex{},
		path:       path,
		pageSize:   pageSize,
	}
}

func (fs *LocalDatabaseFileSystem) Close(path string) error {
	fs.mutex.Lock()
	defer fs.mutex.Unlock()

	file, ok := fs.files[path]

	if !ok {
		return os.ErrNotExist
	}

	delete(fs.files, path)

	return file.Close()
}

func (fs *LocalDatabaseFileSystem) Delete(path string) error {
	fs.mutex.Lock()
	defer fs.mutex.Unlock()

	file, ok := fs.files[path]

	if ok {
		delete(fs.files, path)
		file.Close()
	}

	os.Remove(fmt.Sprintf("%s/%s", fs.path, path))

	return nil
}

// No-op
func (fs *LocalDatabaseFileSystem) FetchPage(pageNumber int64) ([]byte, error) {
	return nil, nil
}

func (fs *LocalDatabaseFileSystem) Open(path string) (internalStorage.File, error) {
	file, err := fs.filesystem.OpenFile(
		fmt.Sprintf("%s/%s", fs.path, path),
		os.O_RDWR|os.O_CREATE,
		0644,
	)

	if err != nil {
		return nil, err
	}

	fs.mutex.Lock()
	fs.files[path] = file
	fs.mutex.Unlock()

	return file, nil
}

// No-op
func (fs *LocalDatabaseFileSystem) PageCache() *PageCache {
	return nil
}

func (fs *LocalDatabaseFileSystem) PageSize() int64 {
	return fs.pageSize
}

func (fs *LocalDatabaseFileSystem) Path() string {
	return fs.path
}

func (fs *LocalDatabaseFileSystem) ReadAt(path string, offset, len int64) ([]byte, error) {
	fs.mutex.RLock()
	file, ok := fs.files[path]
	fs.mutex.RUnlock()

	if !ok {
		return nil, os.ErrNotExist
	}

	var data = make([]byte, len)

	n, err := file.ReadAt(data, offset)

	if err != nil {
		return nil, err
	}

	if int64(n) != len {
		return nil, fmt.Errorf("ReadAt: short read: got %d, expected %d", n, len)
	}

	return data, nil
}

func (fs *LocalDatabaseFileSystem) WriteAt(path string, data []byte, offset int64) (n int, err error) {
	fs.mutex.RLock()
	file, ok := fs.files[path]
	fs.mutex.RUnlock()

	if !ok {
		return 0, os.ErrNotExist
	}

	return file.WriteAt(data, offset)
}

func (fs *LocalDatabaseFileSystem) Size(path string) (int64, error) {
	stat, err := os.Stat(fmt.Sprintf("%s/%s", fs.path, path))

	if err != nil {
		return 0, err
	}

	return stat.Size(), nil
}

func (fs *LocalDatabaseFileSystem) Truncate(path string, size int64) error {
	err := os.Truncate(fmt.Sprintf("%s/%s", fs.path, path), size)

	if err != nil {
		return err
	}

	return nil
}
