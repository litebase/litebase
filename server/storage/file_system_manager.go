package storage

import "sync"

var defaultFileSystem *FileSystem
var fileSystemMutex = &sync.Mutex{}

func FS() *FileSystem {
	fileSystemMutex.Lock()
	defer fileSystemMutex.Unlock()

	if defaultFileSystem == nil {
		defaultFileSystem = NewFileSystem()
	}

	return defaultFileSystem
}
