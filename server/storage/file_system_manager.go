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
		localFileSystem = NewFileSystem(NewLocalFileSystemDriver(fmt.Sprintf("%s/%s", config.Get().DataPath, config.STORAGE_MODE_LOCAL)))
	}

	return localFileSystem
}

/*
The Object FileSystem is used to reada and write objects directly to the object
storage. Using this file system should be avoided in cases where the files being
accessed need high performance or strong consistency guarantees.
*/
func ObjectFS() *FileSystem {
	fileSystemMutex.Lock()
	defer fileSystemMutex.Unlock()

	if objectFileSystem == nil {
		if config.Get().StorageObjectMode == config.STORAGE_MODE_LOCAL {
			objectFileSystem = NewFileSystem(NewLocalFileSystemDriver(fmt.Sprintf("%s/%s", config.Get().DataPath, config.STORAGE_MODE_OBJECT)))
		} else {
			objectFileSystem = NewFileSystem(NewObjectFileSystemDriver())
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

/*
The tiered file system is used to read and write files to a local file system
and an object storage system. The local file system is used as a cache for the
object storage system. The tiered file system will read files from the local
file system if they exist, otherwise it will read them from the object storage
system. When writing files, the tiered file system will write to both the local
file system and the object storage system.
*/
func TieredFS() *FileSystem {
	fileSystemMutex.Lock()
	defer fileSystemMutex.Unlock()

	if tieredFileSystem == nil {
		if config.Get().StorageTieredMode == config.STORAGE_MODE_OBJECT {
			tieredFileSystem = NewFileSystem(
				NewTieredFileSystemDriver(
					GetStorageContext(),
					NewLocalFileSystemDriver(fmt.Sprintf("%s/%s", config.Get().DataPath, "tiered")),
					NewObjectFileSystemDriver(),
				),
			)
		} else if config.Get().StorageTieredMode == config.STORAGE_MODE_DISTRIBUTED {
			tieredFileSystem = NewFileSystem(
				NewDistributedFileSystemDriver(
					GetStorageContext(),
					NewLocalFileSystemDriver(fmt.Sprintf("%s/%s", config.Get().DataPath, "distributed_cache")),
				),
			)
		} else if config.Get().StorageTieredMode == config.STORAGE_MODE_LOCAL {
			tieredFileSystem = NewFileSystem(
				NewTieredFileSystemDriver(
					GetStorageContext(),
					NewLocalFileSystemDriver(fmt.Sprintf("%s/%s", config.Get().DataPath, "tiered")),
					NewLocalFileSystemDriver(fmt.Sprintf("%s/%s", config.Get().DataPath, config.STORAGE_MODE_OBJECT)),
				),
			)
		}
	}

	return tieredFileSystem
}
