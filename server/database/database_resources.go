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

type DatabaseResourceManager struct {
	snapshotLoggers map[string]*backups.SnapshotLogger
	checkpointers   map[string]*Checkpointer
	fileSystems     map[string]*storage.DurableDatabaseFileSystem
	mutex           *sync.Mutex
	rollbackLoggers map[string]*backups.RollbackLogger
	tempFileSystems map[string]*storage.TempDatabaseFileSystem
}

var databaseResourceManager *DatabaseResourceManager
var databaseResourceManagerMutex = &sync.Mutex{}

func DatabaseResources() *DatabaseResourceManager {
	databaseResourceManagerMutex.Lock()
	defer databaseResourceManagerMutex.Unlock()

	if databaseResourceManager == nil {
		databaseResourceManager = &DatabaseResourceManager{
			checkpointers:   map[string]*Checkpointer{},
			fileSystems:     map[string]*storage.DurableDatabaseFileSystem{},
			mutex:           &sync.Mutex{},
			rollbackLoggers: map[string]*backups.RollbackLogger{},
			tempFileSystems: map[string]*storage.TempDatabaseFileSystem{},
		}
	}

	return databaseResourceManager
}

func (d *DatabaseResourceManager) Checkpointer(databaseUuid, branchUuid string) (*Checkpointer, error) {
	d.mutex.Lock()

	key := databaseUuid + ":" + branchUuid

	if checkpointer, ok := d.checkpointers[key]; ok {
		d.mutex.Unlock()
		return checkpointer, nil
	}

	// Always unlock the mutex before creating a new checkpointer to avoid a
	// deadlock when getting the FileSystem.
	d.mutex.Unlock()

	checkpointer, err := NewCheckpointer(
		d.FileSystem(databaseUuid, branchUuid),
		databaseUuid,
		branchUuid,
	)

	if err != nil {
		return nil, err
	}

	d.mutex.Lock()
	d.checkpointers[key] = checkpointer
	d.mutex.Unlock()

	return checkpointer, nil
}

func (d *DatabaseResourceManager) FileSystem(databaseUuid, branchUuid string) *storage.DurableDatabaseFileSystem {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	hash := file.DatabaseHash(databaseUuid, branchUuid)

	if fileSystem, ok := d.fileSystems[hash]; ok {
		return fileSystem
	}

	pageSize := config.Get().PageSize

	fileSystem := storage.NewDurableDatabaseFileSystem(
		storage.TieredFS(),
		fmt.Sprintf("%s%s/%s", Directory(), databaseUuid, branchUuid),
		databaseUuid,
		branchUuid,
		pageSize,
	)

	fileSystem = fileSystem.WithWriteHook(func(offset int64, data []byte) {
		checkpointer, err := d.Checkpointer(databaseUuid, branchUuid)

		if err != nil {
			log.Println("Error creating checkpointer", err)
			return
		}
		log.Println("Writing page to file system", file.PageNumber(offset, pageSize))

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

	d.fileSystems[hash] = fileSystem

	return fileSystem
}

func (d *DatabaseResourceManager) PageLogger(databaseUuid, branchUuid string) *backups.RollbackLogger {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	hash := file.DatabaseHash(databaseUuid, branchUuid)

	if pageLogger, ok := d.rollbackLoggers[hash]; ok {
		return pageLogger
	}

	pageLogger := backups.NewRollbackLogger(databaseUuid, branchUuid)

	d.rollbackLoggers[hash] = pageLogger

	return pageLogger
}

func (d *DatabaseResourceManager) Remove(databaseUuid, branchUuid string) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	hash := file.DatabaseHash(databaseUuid, branchUuid)

	if pageLogger, ok := d.rollbackLoggers[hash]; ok {
		pageLogger.Close()
	}

	// Perform any shutdown logic for the checkpoint logger
	if d.snapshotLoggers[hash] != nil {
		d.snapshotLoggers[hash].Close()
	}

	// Perform any shutdown logic for the file system
	if d.fileSystems[hash] != nil {
		d.fileSystems[hash].Shutdown()
	}

	delete(d.snapshotLoggers, hash)
	delete(d.checkpointers, hash)
	delete(d.fileSystems, hash)
	delete(d.rollbackLoggers, hash)
	delete(d.tempFileSystems, hash)
}

func (d *DatabaseResourceManager) SnapshotLogger(databaseUuid, branchUuid string) *backups.SnapshotLogger {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	hash := file.DatabaseHash(databaseUuid, branchUuid)

	if snapshotLogger, ok := d.snapshotLoggers[hash]; ok {
		return snapshotLogger
	}

	snapshotLogger := backups.NewSnapshotLogger(databaseUuid, branchUuid)

	d.snapshotLoggers[hash] = snapshotLogger

	return snapshotLogger
}

func (d *DatabaseResourceManager) TempFileSystem(databaseUuid, branchUuid string) *storage.TempDatabaseFileSystem {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	hash := file.DatabaseHash(databaseUuid, branchUuid)

	if fileSystem, ok := d.tempFileSystems[hash]; ok {
		return fileSystem
	}

	path := fmt.Sprintf("%s%s/%s/%s", TmpDirectory(), node.Node().Id, databaseUuid, branchUuid)

	fileSystem := storage.NewTempDatabaseFileSystem(path, databaseUuid, branchUuid)

	d.tempFileSystems[hash] = fileSystem

	return fileSystem
}
