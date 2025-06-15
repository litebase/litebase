package database

import (
	"errors"
	"log"
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
	databaseWALManager, err := d.databaseManager.Resources(databaseId, branchId).DatabaseWALManager()

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
	databaseWALManager, err := d.databaseManager.Resources(databaseId, branchId).DatabaseWALManager()

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
	databaseId, branchId string,
	header []byte,
) error {
	databaseWALManager, err := d.databaseManager.Resources(databaseId, branchId).DatabaseWALManager()

	if err != nil {
		log.Println(err)

		return err
	}

	if databaseWALManager == nil {
		log.Println(ErrDatabaseWALNotFound)
		return ErrDatabaseWALNotFound
	}

	// return databaseWALManager.SetWALIndexHeader(header)
	return nil
}
