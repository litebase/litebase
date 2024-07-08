package storage

import (
	"fmt"
	internalStorage "litebase/internal/storage"
	"log"
	"os"
	"sync"
)

type TempDatabaseFileSystem struct {
	files      map[string]internalStorage.File
	fileSystem *LocalFileSystemDriver
	mutex      *sync.RWMutex
	path       string
	pageSize   int64
	writeHook  func(offset int64)
}

func NewTempDatabaseFileSystem(path, databaseUuid, branchUuid string, pageSize int64) *TempDatabaseFileSystem {
	fs := NewLocalFileSystemDriver()

	// Check if the the directory exists
	if _, err := fs.Stat(path); err != nil {
		if os.IsNotExist(err) {
			// Create the directory
			if err := fs.MkdirAll(path, 0755); err != nil {
				log.Fatalln("Error creating temp file system directory", err)
			}
		} else {
			log.Fatalln("Error checking temp file system directory", err)
		}
	}

	return &TempDatabaseFileSystem{
		files:      make(map[string]internalStorage.File),
		fileSystem: fs,
		mutex:      &sync.RWMutex{},
		path:       path,
		pageSize:   pageSize,
	}
}

func (tfs *TempDatabaseFileSystem) Close(path string) error {
	tfs.mutex.Lock()
	defer tfs.mutex.Unlock()

	file, ok := tfs.files[path]

	if !ok {
		return os.ErrNotExist
	}

	delete(tfs.files, path)

	return file.Close()
}

func (tfs *TempDatabaseFileSystem) Delete(path string) error {
	tfs.mutex.Lock()
	defer tfs.mutex.Unlock()

	file, ok := tfs.files[path]

	if ok {
		delete(tfs.files, path)
		file.Close()
	}

	tfs.fileSystem.Remove(fmt.Sprintf("%s/%s", tfs.path, path))

	return nil
}

func (tfs *TempDatabaseFileSystem) Exists() bool {
	_, err := tfs.fileSystem.Stat(tfs.path)

	return err == nil
}

func (tfs *TempDatabaseFileSystem) Open(path string) (internalStorage.File, error) {
	file, err := tfs.fileSystem.OpenFile(
		fmt.Sprintf("%s/%s", tfs.path, path),
		os.O_RDWR|os.O_CREATE,
		0644,
	)

	if err != nil {
		return nil, err
	}

	tfs.mutex.Lock()
	tfs.files[path] = file
	tfs.mutex.Unlock()

	return file, nil
}

func (tfs *TempDatabaseFileSystem) Path() string {
	return tfs.path
}

func (tfs *TempDatabaseFileSystem) ReadAt(path string, offset, len int64) ([]byte, error) {
	tfs.mutex.RLock()
	file, ok := tfs.files[path]
	tfs.mutex.RUnlock()

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

func (tfs *TempDatabaseFileSystem) Size(path string) (int64, error) {
	stat, err := tfs.fileSystem.Stat(fmt.Sprintf("%s/%s", tfs.path, path))

	if err != nil {
		return 0, err
	}

	return stat.Size, nil
}

func (tfs *TempDatabaseFileSystem) Truncate(path string, size int64) error {
	err := tfs.fileSystem.Truncate(fmt.Sprintf("%s/%s", tfs.path, path), size)

	if err != nil {
		return err
	}

	return nil
}

func (tfs *TempDatabaseFileSystem) WithWriteHook(hook func(offset int64)) DatabaseFileSystem {
	tfs.writeHook = hook

	return tfs
}

func (tfs *TempDatabaseFileSystem) WriteAt(path string, data []byte, offset int64) (n int, err error) {
	tfs.mutex.RLock()
	file, ok := tfs.files[path]
	tfs.mutex.RUnlock()

	if !ok {
		return 0, os.ErrNotExist
	}

	n, err = file.WriteAt(data, offset)

	if err != nil {
		return 0, err
	}

	if tfs.writeHook != nil {
		tfs.writeHook(offset)
	}

	return
}
