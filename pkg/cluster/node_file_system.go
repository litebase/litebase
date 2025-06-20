package cluster

import (
	"context"
	"fmt"
	"log"
	"log/slog"

	"github.com/litebase/litebase/pkg/config"
	"github.com/litebase/litebase/pkg/storage"
)

func (cluster *Cluster) LocalFS() *storage.FileSystem {
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

func (cluster *Cluster) NetworkFS() *storage.FileSystem {
	if cluster.networkFileSystem == nil {
		cluster.networkFileSystem = storage.NewFileSystem(
			storage.NewLocalFileSystemDriver(
				cluster.Config.NetworkStoragePath,
			),
		)
	}

	return cluster.networkFileSystem
}

func (cluster *Cluster) ShutdownStorage() {
	if cluster.localFileSystem != nil {
		err := cluster.localFileSystem.Shutdown()

		if err != nil {
			slog.Error("Shutting down local file system", "error", err)
		}
	}

	if cluster.objectFileSystem != nil {
		err := cluster.objectFileSystem.Shutdown()

		if err != nil {
			slog.Error("Shutting down object file system", "error", err)
		}
	}

	if cluster.networkFileSystem != nil {
		err := cluster.networkFileSystem.Shutdown()

		if err != nil {
			slog.Error("Shutting down network file system", "error", err)
		}
	}

	if cluster.tieredFileSystem != nil {
		err := cluster.tieredFileSystem.Shutdown()

		if err != nil {
			slog.Error("Shutting down tiered file system", "error", err)
		}
	}

	if cluster.tmpTieredFileSystem != nil {
		err := cluster.tmpTieredFileSystem.ClearFiles()

		if err != nil {
			log.Println("Clearing tmp tiered file system", err)
		}

		err = cluster.tmpTieredFileSystem.Shutdown()

		if err != nil {
			slog.Error("Shutting down tmp tiered file system", "error", err)
		}
	}

	if cluster.tmpFileSystem != nil {
		err := cluster.tmpFileSystem.ClearFiles()

		if err != nil {
			slog.Debug("Clearing tmp file system", "error", err)
		}

		err = cluster.tmpFileSystem.Shutdown()

		if err != nil {
			slog.Error("Shutting down tmp file system", "error", err)
		}
	}
}

// The tiered file system is used to read and write files to a shared file system
// and an object storage system. The shared file system is used as a cache for the
// object storage system. The tiered file system will read files from the shared
// file system if they exist, otherwise it will read them from the object storage
// system. When writing files, the tiered file system will write to both the shared
// file system and the object storage system.
func (cluster *Cluster) TieredFS() *storage.FileSystem {
	fileSyncEligibilityFn := func(ctx context.Context, fsd *storage.TieredFileSystemDriver) {
		fsd.CanSyncDirtyFiles = func() bool {
			return cluster.Node().Membership == ClusterMembershipPrimary
		}
	}

	if cluster.tieredFileSystem == nil {
		switch cluster.Config.StorageTieredMode {
		case config.StorageModeObject:
			cluster.tieredFileSystem = storage.NewFileSystem(
				storage.NewTieredFileSystemDriver(
					cluster.Node().Context(),
					storage.NewLocalFileSystemDriver(cluster.Config.NetworkStoragePath),
					storage.NewObjectFileSystemDriver(cluster.Config),
					fileSyncEligibilityFn,
				),
			)
		case config.StorageModeLocal:
			cluster.tieredFileSystem = storage.NewFileSystem(
				storage.NewTieredFileSystemDriver(
					cluster.Node().Context(),
					storage.NewLocalFileSystemDriver(cluster.Config.NetworkStoragePath),
					storage.NewLocalFileSystemDriver(fmt.Sprintf("%s/%s", cluster.Config.DataPath, config.StorageModeObject)),
					fileSyncEligibilityFn,
				),
			)
		}
	}

	return cluster.tieredFileSystem
}

func (cluster *Cluster) TmpFS() *storage.FileSystem {
	if cluster.tmpFileSystem == nil {
		cluster.tmpFileSystem = storage.NewFileSystem(
			storage.NewLocalFileSystemDriver(
				fmt.Sprintf("%s/%s", cluster.Config.TmpPath, cluster.Node().ID),
			),
		)
	}

	return cluster.tmpFileSystem
}

// The tmp tiered file system is used to read and write files to a local file
// system and an object storage system. The local file system is used as a cache
// for the object storage system. The tmp tiered file system will read files from
// the local file system if they exist, otherwise it will read them from the object
// storage system. When writing files, the tmp tiered file system will write to
// both the local file system and the object storage system.
func (cluster *Cluster) TmpTieredFS() *storage.FileSystem {
	if cluster.tmpTieredFileSystem != nil {
		return cluster.tmpTieredFileSystem
	}

	fileSyncEligibilityFn := func(ctx context.Context, fsd *storage.TieredFileSystemDriver) {
		fsd.CanSyncDirtyFiles = func() bool {
			return cluster.Node().Membership == ClusterMembershipPrimary
		}
	}

	switch cluster.Config.StorageTieredMode {
	case config.StorageModeObject:
		cluster.tmpTieredFileSystem = storage.NewFileSystem(
			storage.NewTieredFileSystemDriver(
				cluster.Node().Context(),
				storage.NewLocalFileSystemDriver(
					fmt.Sprintf("%s/%s-tiered", cluster.Config.TmpPath, cluster.Node().ID),
				),
				storage.NewObjectFileSystemDriver(cluster.Config),
				fileSyncEligibilityFn,
			),
		)
	case config.StorageModeLocal:
		cluster.tmpTieredFileSystem = storage.NewFileSystem(
			storage.NewTieredFileSystemDriver(
				cluster.Node().Context(),
				storage.NewLocalFileSystemDriver(
					fmt.Sprintf("%s/%s-tiered", cluster.Config.TmpPath, cluster.Node().ID),
				),
				storage.NewLocalFileSystemDriver(fmt.Sprintf("%s/%s", cluster.Config.DataPath, config.StorageModeObject)),
				fileSyncEligibilityFn,
			),
		)
	}

	return cluster.tmpTieredFileSystem
}
