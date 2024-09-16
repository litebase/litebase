package storage

import (
	"context"
	"fmt"
	"litebase/internal/config"
	"sync"
)

var localFileSystem *FileSystem
var objectFileSystem *FileSystem
var tieredFileSystem *FileSystem
var tmpFileSystem *FileSystem
var fileSystemMutex = &sync.RWMutex{}

// Ensure all file systems are initialized by setting them to nil
func InitFS() {
	localFileSystem = nil
	objectFileSystem = nil
	tieredFileSystem = nil
	tmpFileSystem = nil
}

func LocalFS() *FileSystem {
	fileSystemMutex.Lock()
	defer fileSystemMutex.Unlock()

	if localFileSystem == nil {
		localFileSystem = NewFileSystem(NewLocalFileSystemDriver(fmt.Sprintf("%s/%s", config.Get().DataPath, "local")))
	}

	return localFileSystem
}

func ObjectFS() *FileSystem {
	fileSystemMutex.Lock()
	defer fileSystemMutex.Unlock()

	if objectFileSystem == nil {
		if config.Get().StorageMode == "object" {
			objectFileSystem = NewFileSystem(NewObjectFileSystemDriver())
		} else if config.Get().StorageMode == "local" {
			objectFileSystem = NewFileSystem(NewLocalFileSystemDriver(fmt.Sprintf("%s/%s", config.Get().DataPath, "object")))
		}
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
		if config.Get().StorageMode == "object" {
			tieredFileSystem = NewFileSystem(
				NewTieredFileSystemDriver(
					context.TODO(),
					NewLocalFileSystemDriver(fmt.Sprintf("%s/%s", config.Get().DataPath, "tiered")),
					NewObjectFileSystemDriver(),
				),
			)
		} else if config.Get().StorageMode == "local" {
			tieredFileSystem = NewFileSystem(
				NewTieredFileSystemDriver(
					context.TODO(),
					NewLocalFileSystemDriver(fmt.Sprintf("%s/%s", config.Get().DataPath, "tiered")),
					NewLocalFileSystemDriver(fmt.Sprintf("%s/%s", config.Get().DataPath, "object")),
				),
			)
		}
	}

	return tieredFileSystem
}
