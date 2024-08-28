package storage

import (
	"fmt"
	"litebase/internal/config"
	"sync"
)

var localFileSystem *FileSystem
var objectFileSystem *FileSystem
var tieredFileSystem *FileSystem
var tmpFileSystem *FileSystem
var fileSystemMutex = &sync.RWMutex{}

func LocalFS() *FileSystem {
	fileSystemMutex.RLock()
	defer fileSystemMutex.RUnlock()

	if localFileSystem == nil {
		localFileSystem = NewFileSystem(NewLocalFileSystemDriver(fmt.Sprintf("%s/%s", config.Get().DataPath, "local")))
	}

	return localFileSystem
}

func ObjectFS() *FileSystem {
	fileSystemMutex.Lock()
	defer fileSystemMutex.Unlock()

	if objectFileSystem == nil {
		objectFileSystem = NewFileSystem(NewLocalFileSystemDriver(fmt.Sprintf("%s/%s", config.Get().DataPath, "object")))
	}

	return objectFileSystem
}

func TmpFS() *FileSystem {
	fileSystemMutex.Lock()
	defer fileSystemMutex.Unlock()

	if tmpFileSystem == nil {
		tmpFileSystem = NewFileSystem(NewLocalFileSystemDriver(config.Get().TmpPath))
	}

	return tmpFileSystem
}

func TieredFS() *FileSystem {
	fileSystemMutex.Lock()
	defer fileSystemMutex.Unlock()

	if tieredFileSystem == nil {
		tieredFileSystem = NewFileSystem(NewLocalFileSystemDriver(fmt.Sprintf("%s/%s", config.Get().DataPath, "tiered")))
	}

	return tieredFileSystem
}
