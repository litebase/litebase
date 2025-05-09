package cluster

import (
	"fmt"

	"github.com/litebase/litebase/common/config"
	"github.com/litebase/litebase/server/storage"
)

func (cluster *Cluster) ClearFSFiles() {
	if cluster.Config.StorageTieredMode == config.StorageModeObject && cluster.tieredFileSystem != nil {
		cluster.tieredFileSystem.Driver().(*storage.TieredFileSystemDriver).ClearFiles()
	}
}

func (cluster *Cluster) LocalFS() *storage.FileSystem {
	cluster.fileSystemMutex.Lock()
	defer cluster.fileSystemMutex.Unlock()

	if cluster.localFileSystem == nil {
		cluster.localFileSystem = storage.NewFileSystem(
			storage.NewLocalFileSystemDriver(
				fmt.Sprintf(
					"%s/%s",
					cluster.Config.DataPath,
					config.StorageModeLocal,
				),
			),
		)
	}

	return cluster.localFileSystem
}

// The Object FileSystem is used to reada and write objects directly to the object
// storage. Using this file system should be avoided in cases where the files being
// accessed need high performance or strong consistency guarantees.
func (cluster *Cluster) ObjectFS() *storage.FileSystem {
	cluster.fileSystemMutex.Lock()
	defer cluster.fileSystemMutex.Unlock()

	if cluster.objectFileSystem == nil {
		if cluster.Config.StorageObjectMode == config.StorageModeLocal {
			cluster.objectFileSystem = storage.NewFileSystem(
				storage.NewLocalFileSystemDriver(
					fmt.Sprintf(
						"%s/%s",
						cluster.Config.DataPath,
						config.StorageModeObject,
					),
				),
			)
		} else {
			cluster.objectFileSystem = storage.NewFileSystem(
				storage.NewObjectFileSystemDriver(cluster.Config),
			)
		}
	}

	return cluster.objectFileSystem
}

func (cluster *Cluster) SharedFS() *storage.FileSystem {
	cluster.fileSystemMutex.Lock()
	defer cluster.fileSystemMutex.Unlock()

	if cluster.sharedFileSystem == nil {
		cluster.sharedFileSystem = storage.NewFileSystem(
			storage.NewLocalFileSystemDriver(
				cluster.Config.SharedPath,
			),
		)
	}

	return cluster.sharedFileSystem
}

func (cluster *Cluster) ShutdownStorage() {
	if cluster.localFileSystem != nil {
		cluster.localFileSystem.Shutdown()
	}

	if cluster.objectFileSystem != nil {
		cluster.objectFileSystem.Shutdown()
	}

	if cluster.sharedFileSystem != nil {
		cluster.sharedFileSystem.Shutdown()
	}

	if cluster.tieredFileSystem != nil {
		cluster.tieredFileSystem.Shutdown()
	}

	if cluster.tmpFileSystem != nil {
		cluster.tmpFileSystem.Shutdown()
	}
}

func (cluster *Cluster) TmpFS() *storage.FileSystem {
	cluster.fileSystemMutex.Lock()
	defer cluster.fileSystemMutex.Unlock()

	if cluster.tmpFileSystem == nil {
		cluster.tmpFileSystem = storage.NewFileSystem(
			storage.NewLocalFileSystemDriver(
				fmt.Sprintf("%s/%s", cluster.Config.TmpPath, cluster.Node().Id),
			),
		)
	}

	return cluster.tmpFileSystem
}

// The tiered file system is used to read and write files to a local file system
// and an object storage system. The local file system is used as a cache for the
// object storage system. The tiered file system will read files from the local
// file system if they exist, otherwise it will read them from the object storage
// system. When writing files, the tiered file system will write to both the local
// file system and the object storage system.
func (cluster *Cluster) TieredFS() *storage.FileSystem {
	cluster.fileSystemMutex.Lock()
	defer cluster.fileSystemMutex.Unlock()

	if cluster.tieredFileSystem == nil {
		if cluster.Config.StorageTieredMode == config.StorageModeObject {
			cluster.tieredFileSystem = storage.NewFileSystem(
				storage.NewTieredFileSystemDriver(
					cluster.Node().Context(),
					storage.NewLocalFileSystemDriver(cluster.Config.SharedPath),
					storage.NewObjectFileSystemDriver(cluster.Config),
				),
			)
		} else if cluster.Config.StorageTieredMode == config.StorageModeLocal {
			cluster.tieredFileSystem = storage.NewFileSystem(
				storage.NewTieredFileSystemDriver(
					cluster.Node().Context(),
					storage.NewLocalFileSystemDriver(cluster.Config.SharedPath),
					storage.NewLocalFileSystemDriver(fmt.Sprintf("%s/%s", cluster.Config.DataPath, config.StorageModeObject)),
				),
			)
		}
	}

	return cluster.tieredFileSystem
}
