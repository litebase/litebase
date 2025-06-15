package database

import (
	"fmt"
	"log"
	"sync"

	"github.com/litebase/litebase/common/config"
	"github.com/litebase/litebase/pkg/backups"
	"github.com/litebase/litebase/pkg/sqlite3"
	"github.com/litebase/litebase/server/file"
	"github.com/litebase/litebase/server/storage"
)

type DatabaseResources struct {
	BranchId           string
	checkpointer       *Checkpointer
	config             *config.Config
	DatabaseHash       string
	DatabaseId         string
	databaseManager    *DatabaseManager
	snapshotLogger     *backups.SnapshotLogger
	fileSystem         *storage.DurableDatabaseFileSystem
	mutex              *sync.Mutex
	pageLogger         *storage.PageLogger
	resultPool         *sqlite3.ResultPool
	rollbackLogger     *backups.RollbackLogger
	tieredFS           *storage.FileSystem
	transactionManager *TransactionManager
	tmpFS              *storage.FileSystem
	walManager         *DatabaseWALManager
}

// Return a database checkpointer.
func (d *DatabaseResources) Checkpointer() (*Checkpointer, error) {
	var err error

	d.mutex.Lock()
	defer d.mutex.Unlock()

	if d.checkpointer != nil {
		return d.checkpointer, nil
	}

	// Always unlock the mutex before creating a new checkpointer to avoid a
	// deadlock when getting the FileSystem.
	if d.fileSystem == nil {
		d.fileSystem, err = d.createFileSystem()

		if err != nil {
			return nil, err
		}
	}

	// Avoid lock contention
	if d.fileSystem == nil {
		d.fileSystem, err = d.createFileSystem()

		if err != nil {
			return nil, err
		}
	}

	if d.pageLogger == nil {
		d.pageLogger = d.createPageLogger()
	}

	checkpointer, err := NewCheckpointer(
		d.DatabaseId,
		d.BranchId,
		d.fileSystem,
		d.databaseManager.Cluster.NetworkFS(),
		d.pageLogger,
	)

	if err != nil {
		return nil, err
	}

	d.checkpointer = checkpointer

	return d.checkpointer, nil
}

func (d *DatabaseResources) DatabaseWALManager() (*DatabaseWALManager, error) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	if d.walManager != nil {
		return d.walManager, nil
	}

	var err error

	d.walManager, err = NewDatabaseWALManager(
		d.databaseManager.Cluster.Node(),
		d.databaseManager.ConnectionManager(),
		d.DatabaseId,
		d.BranchId,
		d.databaseManager.Cluster.NetworkFS(),
	)

	return d.walManager, err
}

func (d *DatabaseResources) createFileSystem() (*storage.DurableDatabaseFileSystem, error) {
	if d.pageLogger == nil {
		d.pageLogger = d.createPageLogger()
	}

	pageSize := d.config.PageSize

	d.fileSystem = storage.NewDurableDatabaseFileSystem(
		d.databaseManager.Cluster.TieredFS(),
		d.databaseManager.Cluster.NetworkFS(),
		d.pageLogger,
		fmt.Sprintf("%s%s/%s/", Directory(), d.DatabaseId, d.BranchId),
		d.DatabaseId,
		d.BranchId,
		pageSize,
	)

	d.fileSystem.SetWriteHook(func(offset int64, data []byte) {
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

	return d.fileSystem, nil
}

func (d *DatabaseResources) createPageLogger() *storage.PageLogger {
	return d.databaseManager.PageLogManager().Get(
		d.DatabaseId,
		d.BranchId,
		d.databaseManager.Cluster.NetworkFS(),
	)
}

// Return the file system for the database.
func (d *DatabaseResources) FileSystem() *storage.DurableDatabaseFileSystem {
	var err error

	d.mutex.Lock()
	defer d.mutex.Unlock()

	if d.fileSystem != nil {
		return d.fileSystem
	}

	d.fileSystem, err = d.createFileSystem()

	if err != nil {
		log.Println("Error creating file system", err)

		return nil
	}

	return d.fileSystem
}

func (d *DatabaseResources) PageLogger() *storage.PageLogger {
	if d.pageLogger != nil {
		return d.pageLogger
	}

	d.pageLogger = d.createPageLogger()

	return d.pageLogger
}

// Return the rollback logger for the database.
func (d *DatabaseResources) RollbackLogger() *backups.RollbackLogger {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	if d.rollbackLogger != nil {
		return d.rollbackLogger
	}

	if d.rollbackLogger == nil {
		d.rollbackLogger = backups.NewRollbackLogger(d.tieredFS, d.DatabaseId, d.BranchId)
	}

	return d.rollbackLogger
}

// TODO: Need to investigate how this works separatley from the connections and backups.
// Will the ConnectionManager steal a resource away outside the context of a connection.
// TODO: Need to investigate how this impacts long running transactions
func (d *DatabaseResources) Remove() {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	if d.transactionManager != nil {
		d.transactionManager.Shutdown()
	}

	if d.walManager != nil {
		d.walManager.Shutdown()
	}

	if d.rollbackLogger != nil {
		d.rollbackLogger.Close()
	}

	// Perform any shutdown logic for the checkpoint logger
	if d.snapshotLogger != nil {
		d.snapshotLogger.Close()
	}

	if d.pageLogger != nil {
		d.databaseManager.PageLogManager().Release(d.DatabaseId, d.BranchId)
	}

	// Perform any shutdown logic for the file system
	if d.fileSystem != nil {
		d.fileSystem.Shutdown()
	}

	d.snapshotLogger = nil
	d.checkpointer = nil
	d.fileSystem = nil
	d.resultPool = nil
	d.rollbackLogger = nil
	d.walManager = nil
	d.pageLogger = nil
}

// Return the result pool for the database.
func (d *DatabaseResources) ResultPool() *sqlite3.ResultPool {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	if d.resultPool != nil {
		return d.resultPool
	}

	pool := sqlite3.NewResultPool()

	if d.resultPool == nil {
		d.resultPool = pool
	}

	return d.resultPool
}

// Return the SnapshotLogger for the database.
func (d *DatabaseResources) SnapshotLogger() *backups.SnapshotLogger {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	if d.snapshotLogger != nil {
		return d.snapshotLogger
	}

	d.snapshotLogger = backups.NewSnapshotLogger(d.tieredFS, d.DatabaseId, d.BranchId)

	return d.snapshotLogger
}

func (d *DatabaseResources) TransactionManager() *TransactionManager {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	if d.transactionManager != nil {
		return d.transactionManager
	}

	d.transactionManager = NewTransactionManager(
		d.DatabaseId,
		d.BranchId,
	)

	return d.transactionManager
}
