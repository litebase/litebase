package database

import (
	"fmt"
	"litebase/internal/config"
	"litebase/server/backups"
	"litebase/server/file"
	"litebase/server/node"
	"litebase/server/storage"
	"log"
	"sync"
)

type DatabaseResources struct {
	BranchId       string
	DatabaseHash   string
	DatabaseId     string
	distributedWal *storage.DistributedWal
	snapshotLogger *backups.SnapshotLogger
	checkpointer   *Checkpointer
	fileSystem     *storage.DurableDatabaseFileSystem
	mutex          *sync.RWMutex
	rollbackLogger *backups.RollbackLogger
	tempFileSystem *storage.TempDatabaseFileSystem
	walFile        *storage.WalFile
}

var databaseResourceManagerMutex = &sync.RWMutex{}
var resources = map[string]*DatabaseResources{}

/*
Get the resources for the given database and branch UUIDs. If the resources
have not been created, create them and store them in the resources map.
*/
func Resources(databaseId, branchId string) *DatabaseResources {
	databaseResourceManagerMutex.RLock()

	if resource, ok := resources[file.DatabaseHash(databaseId, branchId)]; ok {
		databaseResourceManagerMutex.RUnlock()

		return resource
	}

	databaseResourceManagerMutex.RUnlock()

	databaseResourceManagerMutex.Lock()
	defer databaseResourceManagerMutex.Unlock()

	resource := &DatabaseResources{
		BranchId:     branchId,
		DatabaseId:   databaseId,
		DatabaseHash: file.DatabaseHash(databaseId, branchId),
		mutex:        &sync.RWMutex{},
	}

	resources[file.DatabaseHash(databaseId, branchId)] = resource

	return resource
}

/*
Shutdown all of the database resources that have been created.
*/
func ShutdownResources() {
	databaseResourceManagerMutex.Lock()
	defer databaseResourceManagerMutex.Unlock()

	for _, resource := range resources {
		resource.Remove()
	}

	resources = map[string]*DatabaseResources{}
}

/*
Return a database checkpointer.
*/
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

/*
Return a distributed write-ahead log of the database.
*/
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
			node.Node().WalReplicator(),
		)
	}

	d.mutex.Unlock()

	return d.distributedWal
}

/*
Return the file system for the database.
*/
func (d *DatabaseResources) FileSystem() *storage.DurableDatabaseFileSystem {
	d.mutex.RLock()

	if d.fileSystem != nil {
		d.mutex.RUnlock()

		return d.fileSystem
	}

	d.mutex.RUnlock()

	pageSize := config.Get().PageSize

	fileSystem := storage.NewDurableDatabaseFileSystem(
		storage.TieredFS(),
		fmt.Sprintf("%s%s/%s", Directory(), d.DatabaseId, d.BranchId),
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

/*
Return the rollback logger for the database.
*/
func (d *DatabaseResources) RollbackLogger() *backups.RollbackLogger {
	d.mutex.RLock()

	if d.rollbackLogger != nil {
		d.mutex.RUnlock()

		return d.rollbackLogger
	}

	d.mutex.RUnlock()

	pageLogger := backups.NewRollbackLogger(d.DatabaseId, d.BranchId)

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

/*
Return the SnapshotLogger for the database.
*/
func (d *DatabaseResources) SnapshotLogger() *backups.SnapshotLogger {
	d.mutex.RLock()

	if d.snapshotLogger != nil {
		d.mutex.RUnlock()

		return d.snapshotLogger

	}

	d.mutex.RUnlock()

	d.mutex.Lock()

	if d.snapshotLogger == nil {
		d.snapshotLogger = backups.NewSnapshotLogger(d.DatabaseId, d.BranchId)
	}

	d.mutex.Unlock()

	return d.snapshotLogger
}

/*
Return the temporary file system for the database.
*/
func (d *DatabaseResources) TempFileSystem() *storage.TempDatabaseFileSystem {
	d.mutex.RLock()

	if d.tempFileSystem != nil {
		d.mutex.RUnlock()

		return d.tempFileSystem
	}

	d.mutex.RUnlock()

	path := fmt.Sprintf("%s%s/%s/%s", TmpDirectory(), node.Node().Id, d.DatabaseId, d.BranchId)

	fileSystem := storage.NewTempDatabaseFileSystem(path, d.DatabaseId, d.BranchId)

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

	path := fmt.Sprintf("%s%s/%s/%s/%s.db-wal", TmpDirectory(), node.Node().Id, d.DatabaseId, d.BranchId, d.DatabaseHash)

	walFile, err := storage.NewWalFile(path)

	if err != nil {
		return nil, err
	}

	d.walFile = walFile

	return walFile, nil
}
