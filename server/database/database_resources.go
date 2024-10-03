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
	BranchUuid     string
	DatabaseUuid   string
	snapshotLogger *backups.SnapshotLogger
	checkpointer   *Checkpointer
	fileSystem     *storage.DurableDatabaseFileSystem
	mutex          *sync.RWMutex
	rollbackLogger *backups.RollbackLogger
	tempFileSystem *storage.TempDatabaseFileSystem
}

var databaseResourceManagerMutex = &sync.RWMutex{}
var resources = map[string]*DatabaseResources{}

func Resources(databaseUuid, branchUuid string) *DatabaseResources {
	databaseResourceManagerMutex.RLock()

	if resource, ok := resources[file.DatabaseHash(databaseUuid, branchUuid)]; ok {
		databaseResourceManagerMutex.RUnlock()

		return resource
	}

	databaseResourceManagerMutex.RUnlock()

	databaseResourceManagerMutex.Lock()
	defer databaseResourceManagerMutex.Unlock()

	resource := &DatabaseResources{
		BranchUuid:   branchUuid,
		DatabaseUuid: databaseUuid,
		mutex:        &sync.RWMutex{},
	}

	resources[file.DatabaseHash(databaseUuid, branchUuid)] = resource

	return resource
}

func ShutdownResources() {
	databaseResourceManagerMutex.Lock()
	defer databaseResourceManagerMutex.Unlock()

	for _, resource := range resources {
		resource.Remove()
	}

	resources = map[string]*DatabaseResources{}
}

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
		d.DatabaseUuid,
		d.BranchUuid,
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
		fmt.Sprintf("%s%s/%s", Directory(), d.DatabaseUuid, d.BranchUuid),
		d.DatabaseUuid,
		d.BranchUuid,
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

		if node.Node().IsPrimary() {
			// node.Node().Publish(
			// 	node.NodeMessage{
			// 		Id:   "broadcast",
			// 		Type: "WALCheckpointMessage",
			// 		Data: node.WALCheckpointMessage{
			// 			DatabaseUuid: databaseUuid,
			// 			BranchUuid:   branchUuid,
			// 			Timestamp:    fileSystem.TransactionTimestamp(),
			// 		},
			// 	},
			// )
		}
	})

	d.mutex.Lock()

	if d.fileSystem == nil {
		d.fileSystem = fileSystem
	}

	d.mutex.Unlock()

	return d.fileSystem
}

func (d *DatabaseResources) RollbackLogger() *backups.RollbackLogger {
	d.mutex.RLock()

	if d.rollbackLogger != nil {
		d.mutex.RUnlock()

		return d.rollbackLogger
	}

	d.mutex.RUnlock()

	pageLogger := backups.NewRollbackLogger(d.DatabaseUuid, d.BranchUuid)

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

func (d *DatabaseResources) SnapshotLogger() *backups.SnapshotLogger {
	d.mutex.RLock()

	if d.snapshotLogger != nil {
		d.mutex.RUnlock()

		return d.snapshotLogger

	}

	d.mutex.RUnlock()

	d.mutex.Lock()

	if d.snapshotLogger == nil {
		d.snapshotLogger = backups.NewSnapshotLogger(d.DatabaseUuid, d.BranchUuid)
	}

	d.mutex.Unlock()

	return d.snapshotLogger
}

func (d *DatabaseResources) TempFileSystem() *storage.TempDatabaseFileSystem {
	d.mutex.RLock()

	if d.tempFileSystem != nil {
		d.mutex.RUnlock()

		return d.tempFileSystem
	}

	d.mutex.RUnlock()

	path := fmt.Sprintf("%s%s/%s/%s", TmpDirectory(), node.Node().Id, d.DatabaseUuid, d.BranchUuid)

	fileSystem := storage.NewTempDatabaseFileSystem(path, d.DatabaseUuid, d.BranchUuid)

	d.mutex.Lock()

	if d.tempFileSystem == nil {
		d.tempFileSystem = fileSystem
	}

	d.mutex.Unlock()

	return d.tempFileSystem
}
