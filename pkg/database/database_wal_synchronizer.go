package database

import (
	"errors"
	"log"

	"github.com/litebase/litebase/pkg/vfs"
)

var ErrDatabaseWALNotFound = errors.New("Database WAL not found")

type DatabaseWalSynchronizer struct {
	databaseManager *DatabaseManager
}

// Create a new instance of the DatabaseWalSynchronizer.
func NewDatabaseWALSynchronizer(databaseManager *DatabaseManager) *DatabaseWalSynchronizer {
	return &DatabaseWalSynchronizer{
		databaseManager: databaseManager,
	}
}

func (d *DatabaseWalSynchronizer) GetActiveWALVersions(databaseId, branchId string) ([]int64, error) {
	branch, err := d.databaseManager.GetBranch(databaseId, branchId)

	if err != nil {
		log.Println(err)

		return nil, err
	}

	databaseWALManager, err := d.databaseManager.Resources(branch).DatabaseWALManager()

	if err != nil {
		log.Println(err)

		return nil, err
	}

	if databaseWALManager == nil {
		log.Println(ErrDatabaseWALNotFound)
		return nil, ErrDatabaseWALNotFound
	}

	return databaseWALManager.InUseVersions(), nil
}

func (d *DatabaseWalSynchronizer) SetCurrentTimestamp(
	databaseId, branchId string,
	timestamp int64,
) error {
	branch, err := d.databaseManager.GetBranch(databaseId, branchId)

	if err != nil {
		log.Println(err)

		return err
	}

	databaseWALManager, err := d.databaseManager.Resources(branch).DatabaseWALManager()

	if err != nil {
		log.Println(err)

		return err
	}

	if databaseWALManager == nil {
		log.Println(ErrDatabaseWALNotFound)
		return ErrDatabaseWALNotFound
	}

	// databaseWal.Index().SetCurrentTimestamp(timestamp)

	return nil
}

func (d *DatabaseWalSynchronizer) SetWALIndexHeader(
	databaseId string,
	branchId string,
	databaseHash string,
	nodeHash string,
	timestamp int64,
	header []byte,
) error {
	branch, err := d.databaseManager.GetBranch(databaseId, branchId)

	if err != nil {
		log.Println(err)

		return err
	}

	databaseWALManager, err := d.databaseManager.Resources(branch).DatabaseWALManager()

	if err != nil {
		log.Println(err)

		return err
	}

	wal, err := databaseWALManager.Get(timestamp)

	if err != nil {
		log.Println(err)
		return err
	}

	if wal == nil {
		log.Println(ErrDatabaseWALNotFound)
		return ErrDatabaseWALNotFound
	}

	return vfs.UpdateWALSharedMemory(
		databaseWALManager.databaseHash,
		databaseWALManager.nodeHash,
		wal.Timestamp(),
		header,
	)
}
