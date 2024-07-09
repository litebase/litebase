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

type RemoteDatabaseFileSystem struct {
	files      map[string]internalStorage.File
	fileSystem *FileSystem
	hasPageOne bool
	mutex      *sync.RWMutex
	path       string
	pageSize   int64
	size       int64
	writeHook  func(offset int64)
}

func NewRemoteDatabaseFileSystem(path, databaseUuid, branchUuid string, pageSize int64) *RemoteDatabaseFileSystem {
	// Check if the the directory exists
	if _, err := FS().Stat(path); err != nil {
		if os.IsNotExist(err) {
			// Create the directory
			if err := FS().MkdirAll(path, 0755); err != nil {
				log.Fatalln("Error creating temp file system directory", err)
			}
		} else {
			log.Fatalln("Error checking temp file system directory", err)
		}
	}

	return &RemoteDatabaseFileSystem{
		files:      make(map[string]internalStorage.File),
		fileSystem: FS(),
		mutex:      &sync.RWMutex{},
		path:       path,
		pageSize:   pageSize,
	}
}

func (rfs *RemoteDatabaseFileSystem) Close(path string) error {
	rfs.mutex.Lock()
	defer rfs.mutex.Unlock()

	file, ok := rfs.files[path]

	if !ok {
		return os.ErrNotExist
	}

	return file.Close()
}

func (rfs *RemoteDatabaseFileSystem) Delete(path string) error {
	rfs.mutex.Lock()
	defer rfs.mutex.Unlock()

	file, ok := rfs.files[path]

	if ok {
		delete(rfs.files, path)
		file.Close()
	}

	rfs.fileSystem.Remove(fmt.Sprintf("%s/%s", rfs.path, path))

	return nil
}

func (rfs *RemoteDatabaseFileSystem) Exists() bool {
	_, err := rfs.fileSystem.Stat(rfs.path)

	return err == nil
}

func (rfs *RemoteDatabaseFileSystem) Open(path string) (internalStorage.File, error) {
	// Create a directory for the given path
	path = fmt.Sprintf("%s/%s", rfs.path, strings.ReplaceAll(path, ".db", ""))

	err := rfs.fileSystem.MkdirAll(path, 0755)

	if err != nil {
		return nil, err
	}

	return nil, nil
}

func (rfs *RemoteDatabaseFileSystem) Path() string {
	return rfs.path
}

func (rfs *RemoteDatabaseFileSystem) ReadAt(path string, offset, length int64) ([]byte, error) {
	pageNumber := file.PageNumber(offset, rfs.pageSize)
	path = strings.ReplaceAll(path, ".db", "")
	data, err := rfs.fileSystem.ReadFile(fmt.Sprintf("%s/%s/%010d", rfs.path, path, pageNumber))

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

	if len(data) == int(rfs.pageSize) && pageNumber == 1 {
		rfs.hasPageOne = true
	}

	return data, nil
}

// TODO: this should use the metadata file to get the size
func (rfs *RemoteDatabaseFileSystem) Size(path string) (int64, error) {
	if rfs.hasPageOne {
		rfs.size = rfs.pageSize * 4294967294
	}

	return rfs.size, nil
}

func (rfs *RemoteDatabaseFileSystem) Truncate(path string, size int64) error {
	path = strings.ReplaceAll(path, ".db", "")

	// No-op since pages are stored in separate files and we don't need to
	// truncate the database "file" to a certain size.
	if size > 0 {
		return nil
	}

	// Remove all the files from the directory
	err := rfs.fileSystem.RemoveAll(fmt.Sprintf("%s/%s", rfs.path, path))

	if err != nil {
		return err
	}

	err = rfs.fileSystem.MkdirAll(fmt.Sprintf("%s/%s", rfs.path, path), 0755)

	if err != nil {
		return err
	}

	return nil
}

func (rfs *RemoteDatabaseFileSystem) WithWriteHook(hook func(offset int64)) DatabaseFileSystem {
	rfs.writeHook = hook

	return rfs
}

func (rfs *RemoteDatabaseFileSystem) WriteAt(path string, data []byte, offset int64) (n int, err error) {
	compressed := pgEncoder().EncodeAll(data, nil)
	pageNumber := file.PageNumber(offset, rfs.pageSize)
	path = strings.ReplaceAll(path, ".db", "")
	err = rfs.fileSystem.WriteFile(fmt.Sprintf("%s/%s/%010d", rfs.path, path, pageNumber), compressed, 0644)

	if err != nil {
		return 0, err
	}

	if rfs.writeHook != nil {
		rfs.writeHook(offset)
	}

	return len(data), nil
}
