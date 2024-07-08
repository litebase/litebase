package storage

import (
	"fmt"
	internalStorage "litebase/internal/storage"
	"litebase/server/file"
	"log"
	"os"
	"strings"
	"sync"
)

type LocalDatabaseFileSystem struct {
	files      map[string]internalStorage.File
	fileSystem *LocalFileSystemDriver
	hasPageOne bool
	mutex      *sync.RWMutex
	path       string
	pageSize   int64
	size       int64
	writeHook  func(offset int64)
}

func NewLocalDatabaseFileSystem(path, databaseUuid, branchUuid string, pageSize int64) *LocalDatabaseFileSystem {
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

	return &LocalDatabaseFileSystem{
		files:      make(map[string]internalStorage.File),
		fileSystem: fs,
		mutex:      &sync.RWMutex{},
		path:       path,
		pageSize:   pageSize,
	}
}

func (lfs *LocalDatabaseFileSystem) Close(path string) error {
	lfs.mutex.Lock()
	defer lfs.mutex.Unlock()

	file, ok := lfs.files[path]

	if !ok {
		return os.ErrNotExist
	}

	delete(lfs.files, path)

	return file.Close()
}

func (lfs *LocalDatabaseFileSystem) Delete(path string) error {
	lfs.mutex.Lock()
	defer lfs.mutex.Unlock()

	file, ok := lfs.files[path]

	if ok {
		delete(lfs.files, path)
		file.Close()
	}

	lfs.fileSystem.Remove(fmt.Sprintf("%s/%s", lfs.path, path))

	return nil
}

func (lfs *LocalDatabaseFileSystem) Exists() bool {
	_, err := lfs.fileSystem.Stat(lfs.path)

	return err == nil
}

func (lfs *LocalDatabaseFileSystem) Open(path string) (internalStorage.File, error) {
	path = fmt.Sprintf("%s/%s", lfs.path, strings.ReplaceAll(path, ".db", ""))

	err := lfs.fileSystem.MkdirAll(path, 0755)

	if err != nil {
		return nil, err
	}

	return nil, nil
}

func (lfs *LocalDatabaseFileSystem) Path() string {
	return lfs.path
}

func (lfs *LocalDatabaseFileSystem) ReadAt(path string, offset, length int64) ([]byte, error) {
	pageNumber := file.PageNumber(offset, lfs.pageSize)
	path = strings.ReplaceAll(path, ".db", "")
	data, err := lfs.fileSystem.ReadFile(fmt.Sprintf("%s/%s/%010d", lfs.path, path, pageNumber))

	if err != nil {
		if !os.IsNotExist(err) {
			log.Println("Error reading file", err)
			return nil, err
		}

		if os.IsNotExist(err) {
			data = make([]byte, length)
		}
	} else {
		data, err = pgDecoder().DecodeAll(data, nil)

		if err != nil {
			return nil, err
		}
	}

	if len(data) == int(lfs.pageSize) && pageNumber == 1 {
		lfs.hasPageOne = true
	}

	return data, nil
}

// TODO: this should use the metadata file to get the size
func (lfs *LocalDatabaseFileSystem) Size(path string) (int64, error) {
	if lfs.hasPageOne {
		lfs.size = lfs.pageSize * 4294967294
	}

	return lfs.size, nil
}

func (lfs *LocalDatabaseFileSystem) Truncate(path string, size int64) error {
	path = strings.ReplaceAll(path, ".db", "")

	// No-op since pages are stored in separate files and we don't need to
	// truncate the database "file" to a certain size.
	if size > 0 {
		return nil
	}

	// Remove all the files from the directory
	err := lfs.fileSystem.RemoveAll(fmt.Sprintf("%s/%s", lfs.path, path))

	if err != nil {
		return err
	}

	err = lfs.fileSystem.MkdirAll(fmt.Sprintf("%s/%s", lfs.path, path), 0755)

	if err != nil {
		return err
	}

	return nil
}

func (lfs *LocalDatabaseFileSystem) WithWriteHook(hook func(offset int64)) DatabaseFileSystem {
	lfs.writeHook = hook

	return lfs
}

func (lfs *LocalDatabaseFileSystem) WriteAt(path string, data []byte, offset int64) (n int, err error) {
	compressed := pgEncoder().EncodeAll(data, nil)
	pageNumber := file.PageNumber(offset, lfs.pageSize)
	path = strings.ReplaceAll(path, ".db", "")
	err = lfs.fileSystem.WriteFile(fmt.Sprintf("%s/%s/%010d", lfs.path, path, pageNumber), compressed, 0644)

	if err != nil {
		return 0, err
	}

	if lfs.writeHook != nil {
		lfs.writeHook(offset)
	}

	return len(data), nil
}
