package database

import (
	"fmt"
	"litebasedb/internal/config"
	"litebasedb/server/backups"
	"litebasedb/server/file"
	"litebasedb/server/storage"
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

func (d *DatabaseResourceManager) Checkpointer(databaseUuid, branchUuid string) *Checkpointer {
	d.mutext.Lock()
	defer d.mutext.Unlock()

	hash := file.DatabaseHash(databaseUuid, branchUuid)

	if checkpointer, ok := d.checkpointers[hash]; ok {
		return checkpointer
	}

	checkpointer := NewCheckpointer(databaseUuid, branchUuid)

	d.checkpointers[hash] = checkpointer

	return checkpointer
}

func (d *DatabaseResourceManager) FileSystem(databaseUuid, branchUuid string) storage.DatabaseFileSystem {
	d.mutext.Lock()
	defer d.mutext.Unlock()
	hash := file.DatabaseHash(databaseUuid, branchUuid)

	if fileSystem, ok := d.fileSystems[hash]; ok {
		return fileSystem
	}

	pageSize := config.Get().PageSize

	fileSystem := storage.NewLocalDatabaseFileSystem(
		fmt.Sprintf("%s/%s/%s", Directory(), databaseUuid, branchUuid),
		databaseUuid,
		branchUuid,
		pageSize,
	).WithWriteHook(func(offset int64) {
		// Each time a page is written, we need to inform the check pointer to
		// ensure it is included in the next backup.
		d.Checkpointer(databaseUuid, branchUuid).AddPage(
			uint32(file.PageNumber(offset, pageSize)),
		)
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

func (d *DatabaseResourceManager) TempFileSystem(databaseUuid, branchUuid string) storage.DatabaseFileSystem {
	d.mutext.Lock()
	defer d.mutext.Unlock()

	hash := file.DatabaseHash(databaseUuid, branchUuid)

	if fileSystem, ok := d.tempFileSystems[hash]; ok {
		return fileSystem
	}

	fileSystem := storage.NewLocalDatabaseFileSystem(
		fmt.Sprintf("%s/%s/%s", Directory(), databaseUuid, branchUuid),
		databaseUuid,
		branchUuid,
		config.Get().PageSize,
	)

	d.tempFileSystems[hash] = fileSystem

	return fileSystem
}

func (d *DatabaseResourceManager) Remove(databaseUuid, branchUuid string) {
	d.mutext.Lock()
	defer d.mutext.Unlock()

	hash := file.DatabaseHash(databaseUuid, branchUuid)

	delete(d.checkpointLoggers, hash)
	delete(d.checkpointers, hash)
	delete(d.fileSystems, hash)
	delete(d.pageLoggers, hash)
	delete(d.tempFileSystems, hash)
}
