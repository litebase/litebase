package database

import (
	"fmt"
	"litebase/internal/config"
	"litebase/server/backups"
	"litebase/server/file"
	"litebase/server/storage"
	"log"
	"sync"
)

type DatabaseResources struct {
	BranchId        string
	config          *config.Config
	DatabaseHash    string
	DatabaseId      string
	databaseManager *DatabaseManager
	distributedWal  *storage.DistributedWal
	snapshotLogger  *backups.SnapshotLogger
	checkpointer    *Checkpointer
	fileSystem      *storage.DurableDatabaseFileSystem
	mutex           *sync.RWMutex
	rollbackLogger  *backups.RollbackLogger
	tempFileSystem  *storage.TempDatabaseFileSystem
	tieredFS        *storage.FileSystem
	tmpFS           *storage.FileSystem
	walFile         *storage.WalFile
}

// Return a database checkpointer.
func (d *DatabaseResources) Checkpointer() (*Checkpointer, error) {
	d.mutex.RLock()

	if d.checkpointer != nil {
		d.mutex.RUnlock()
		return d.checkpointer, nil
	}

	// Always unlock the mutex before creating a new checkpointer to avoid a
	// deadlock when getting the FileSystem.
	d.mutex.RUnlock()

	checkpointer, err := NewCheckpointer(
		d.DatabaseId,
		d.BranchId,
		d.FileSystem(),
	)

	if err != nil {
		return nil, err
	}

	d.mutex.Lock()

	if d.checkpointer == nil {
		d.checkpointer = checkpointer
	}

	d.mutex.Unlock()

	return d.checkpointer, nil
}

// Return a distributed write-ahead log of the database.
func (d *DatabaseResources) DistributedWal() *storage.DistributedWal {
	d.mutex.RLock()

	if d.distributedWal != nil {
		d.mutex.RUnlock()

		return d.distributedWal
	}

	d.mutex.RUnlock()

	d.mutex.Lock()

	if d.distributedWal == nil {
		d.distributedWal = storage.NewDistributedWal(
			d.DatabaseId,
			d.BranchId,
			d.databaseManager.Cluster.Node().WalReplicator(),
		)
	}

	d.mutex.Unlock()

	return d.distributedWal
}

// Return the file system for the database.
func (d *DatabaseResources) FileSystem() *storage.DurableDatabaseFileSystem {
	d.mutex.RLock()

	if d.fileSystem != nil {
		d.mutex.RUnlock()

		return d.fileSystem
	}

	d.mutex.RUnlock()

	pageSize := d.config.PageSize

	fileSystem := storage.NewDurableDatabaseFileSystem(
		d.tieredFS,
		fmt.Sprintf("%s%s/%s/", Directory(), d.DatabaseId, d.BranchId),
		d.DatabaseId,
		d.BranchId,
		pageSize,
	)

	fileSystem = fileSystem.SetWriteHook(func(offset int64, data []byte) {
		checkpointer, err := d.Checkpointer()

		if err != nil {
			log.Println("Error creating checkpointer", err)
			return
		}

		// Each time a page is written, we need to inform the check pointer to
		// ensure it is included in the next backup.
		checkpointer.CheckpointPage(
			file.PageNumber(offset, pageSize),
			data,
		)
	})

	d.mutex.Lock()

	if d.fileSystem == nil {
		d.fileSystem = fileSystem
	}

	d.mutex.Unlock()

	return d.fileSystem
}

// Return the rollback logger for the database.
func (d *DatabaseResources) RollbackLogger() *backups.RollbackLogger {
	d.mutex.RLock()

	if d.rollbackLogger != nil {
		d.mutex.RUnlock()

		return d.rollbackLogger
	}

	d.mutex.RUnlock()

	pageLogger := backups.NewRollbackLogger(d.tieredFS, d.DatabaseId, d.BranchId)

	d.mutex.Lock()

	if d.rollbackLogger == nil {
		d.rollbackLogger = pageLogger
	}

	d.mutex.Unlock()

	return d.rollbackLogger
}

// TODO: Need to investigate how this works separatley from the connections and backups.
// Will the ConnectionManager steal a resource away outside the context of a connection.
func (d *DatabaseResources) Remove() {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	if d.rollbackLogger != nil {
		d.rollbackLogger.Close()
	}

	// Perform any shutdown logic for the checkpoint logger
	if d.snapshotLogger != nil {
		d.snapshotLogger.Close()
	}

	// Perform any shutdown logic for the file system
	if d.fileSystem != nil {
		d.fileSystem.Shutdown()
	}

	d.snapshotLogger = nil
	d.checkpointer = nil
	d.fileSystem = nil
	d.rollbackLogger = nil
	d.tempFileSystem = nil
}

// Return the SnapshotLogger for the database.
func (d *DatabaseResources) SnapshotLogger() *backups.SnapshotLogger {
	d.mutex.RLock()

	if d.snapshotLogger != nil {
		d.mutex.RUnlock()

		return d.snapshotLogger

	}

	d.mutex.RUnlock()

	d.mutex.Lock()

	if d.snapshotLogger == nil {
		d.snapshotLogger = backups.NewSnapshotLogger(d.tieredFS, d.DatabaseId, d.BranchId)
	}

	d.mutex.Unlock()

	return d.snapshotLogger
}

// Return the temporary file system for the database.
func (d *DatabaseResources) TempFileSystem() *storage.TempDatabaseFileSystem {
	d.mutex.RLock()

	if d.tempFileSystem != nil {
		d.mutex.RUnlock()

		return d.tempFileSystem
	}

	d.mutex.RUnlock()

	path := fmt.Sprintf(
		"%s%s/%s/%s",
		TmpDirectory(),
		d.databaseManager.Cluster.Node().Id,
		d.DatabaseId,
		d.BranchId,
	)

	fileSystem := storage.NewTempDatabaseFileSystem(d.tmpFS, path, d.DatabaseId, d.BranchId)

	d.mutex.Lock()

	if d.tempFileSystem == nil {
		d.tempFileSystem = fileSystem
	}

	d.mutex.Unlock()

	return d.tempFileSystem
}

func (d *DatabaseResources) WalFile() (*storage.WalFile, error) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	if d.walFile != nil {
		return d.walFile, nil
	}

	path := fmt.Sprintf(
		"%s%s/%s/%s/%s.db-wal",
		TmpDirectory(),
		d.databaseManager.Cluster.Node().Id,
		d.DatabaseId,
		d.BranchId,
		d.DatabaseHash,
	)

	walFile, err := storage.NewWalFile(d.tmpFS, path)

	if err != nil {
		return nil, err
	}

	d.walFile = walFile

	return walFile, nil
}
