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
	checkpointLoggers map[string]*backups.CheckpointLogger
	checkpointers     map[string]*Checkpointer
	fileSystems       map[string]storage.DatabaseFileSystem
	mutext            *sync.Mutex
	pageLoggers       map[string]*backups.PageLogger
	tempFileSystems   map[string]storage.DatabaseFileSystem
}

var databaseResourceManager *DatabaseResourceManager
var databaseResourceManagerMutex = &sync.Mutex{}

func DatabaseResources() *DatabaseResourceManager {
	databaseResourceManagerMutex.Lock()
	defer databaseResourceManagerMutex.Unlock()

	if databaseResourceManager == nil {
		databaseResourceManager = &DatabaseResourceManager{
			checkpointers:   map[string]*Checkpointer{},
			fileSystems:     map[string]storage.DatabaseFileSystem{},
			mutext:          &sync.Mutex{},
			pageLoggers:     map[string]*backups.PageLogger{},
			tempFileSystems: map[string]storage.DatabaseFileSystem{},
		}
	}

	return databaseResourceManager
}

func (d *DatabaseResourceManager) CheckpointLogger(databaseUuid, branchUuid string) *backups.CheckpointLogger {
	d.mutext.Lock()
	defer d.mutext.Unlock()

	hash := file.DatabaseHash(databaseUuid, branchUuid)

	if checkpointLogger, ok := d.checkpointLoggers[hash]; ok {
		return checkpointLogger
	}

	checkpointLogger := backups.NewCheckpointLogger(databaseUuid, branchUuid)

	d.checkpointLoggers[hash] = checkpointLogger

	return checkpointLogger
}

func (d *DatabaseResourceManager) Checkpointer(databaseUuid, branchUuid string) (*Checkpointer, error) {
	d.mutext.Lock()

	key := databaseUuid + ":" + branchUuid

	if checkpointer, ok := d.checkpointers[key]; ok {
		d.mutext.Unlock()
		return checkpointer, nil
	}

	// Always unlock the mutex before creating a new checkpointer to avoid a
	// deadlock when getting the FileSystem.
	d.mutext.Unlock()

	checkpointer, err := NewCheckpointer(
		d.FileSystem(databaseUuid, branchUuid),
		databaseUuid,
		branchUuid,
	)

	if err != nil {
		return nil, err
	}

	d.mutext.Lock()
	d.checkpointers[key] = checkpointer
	d.mutext.Unlock()

	return checkpointer, nil
}

func (d *DatabaseResourceManager) FileSystem(databaseUuid, branchUuid string) storage.DatabaseFileSystem {
	d.mutext.Lock()
	defer d.mutext.Unlock()
	hash := file.DatabaseHash(databaseUuid, branchUuid)

	if fileSystem, ok := d.fileSystems[hash]; ok {
		return fileSystem
	}

	pageSize := config.Get().PageSize
	var fileSystem storage.DatabaseFileSystem

	fileSystem = storage.NewDurableDatabaseFileSystem(
		storage.TieredFS(),
		fmt.Sprintf("%s/%s/%s", Directory(), databaseUuid, branchUuid),
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

		// Each time a page is written, we need to inform the check pointer to
		// ensure it is included in the next backup.
		checkpointer.AddPage(
			uint32(file.PageNumber(offset, pageSize)),
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

func (d *DatabaseResourceManager) PageLogger(databaseUuid, branchUuid string) *backups.PageLogger {
	d.mutext.Lock()
	defer d.mutext.Unlock()

	hash := file.DatabaseHash(databaseUuid, branchUuid)

	if pageLogger, ok := d.pageLoggers[hash]; ok {
		return pageLogger
	}

	pageLogger := backups.NewPageLogger(databaseUuid, branchUuid)

	d.pageLoggers[hash] = pageLogger

	return pageLogger
}

func (d *DatabaseResourceManager) Remove(databaseUuid, branchUuid string) {
	d.mutext.Lock()
	defer d.mutext.Unlock()

	hash := file.DatabaseHash(databaseUuid, branchUuid)

	if pageLogger, ok := d.pageLoggers[hash]; ok {
		pageLogger.Close()
	}

	delete(d.checkpointLoggers, hash)
	delete(d.checkpointers, hash)
	delete(d.fileSystems, hash)
	delete(d.pageLoggers, hash)
	delete(d.tempFileSystems, hash)
}

func (d *DatabaseResourceManager) TempFileSystem(databaseUuid, branchUuid string) storage.DatabaseFileSystem {
	d.mutext.Lock()
	defer d.mutext.Unlock()

	hash := file.DatabaseHash(databaseUuid, branchUuid)

	if fileSystem, ok := d.tempFileSystems[hash]; ok {
		return fileSystem
	}

	path := fmt.Sprintf("%s/%s/%s/%s", TmpDirectory(), node.Node().Id, databaseUuid, branchUuid)

	fileSystem := storage.NewTempDatabaseFileSystem(
		path,
		databaseUuid,
		branchUuid,
		config.Get().PageSize,
	)

	// TODO: Define the boundaries of a transaction so we can ship multiple pages at one time.
	fileSystem = fileSystem.WithWriteHook(func(offset int64, data []byte) {
		// Each time a page is written, we will replicate it out to the other
		// nodes. These pages are written in order.
		if node.Node().IsPrimary() {
			// walFile, err := storage.TieredFS().OpenFile(fmt.Sprintf("%s/%s", fileSystem.Path(), path), os.O_RDONLY, 0644)

			// if err != nil {
			// 	log.Println("Error reading file", err, path)
			// 	return
			// }

			// defer walFile.Close()

			// hasher := sha256.New()

			// if _, err := walFile.WriteTo(hasher); err != nil {
			// 	log.Println("Error reading file", err, path)
			// 	return
			// }

			// var fileSha256 [32]byte

			// copy(fileSha256[:], hasher.Sum(nil))

			// // log.Println("Sending WAL replication message", fileSystem.TransactionTimestamp())

			// err = node.Node().Publish(
			// 	node.NodeMessage{
			// 		Id:   "broadcast",
			// 		Type: "WALReplicationMessage",
			// 		Data: node.WALReplicationMessage{
			// 			BranchUuid:   branchUuid,
			// 			DatabaseUuid: databaseUuid,
			// 			Data:         s2.Encode(nil, data),
			// 			Offset:       int(offset),
			// 			Length:       len(data),
			// 			Sha256:       fileSha256,
			// 			Timestamp:    fileSystem.TransactionTimestamp(),
			// 		},
			// 	},
			// )

			// if err != nil {
			// 	log.Println("Failed to publish WAL replication message: ", err)
			// }
		}
	})

	d.tempFileSystems[hash] = fileSystem

	return fileSystem
}
