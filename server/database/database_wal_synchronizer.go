package database

import (
	"log"
)

type DatabaseWalSynchronizer struct {
	databaseManager *DatabaseManager
}

func NewDatabaseWalSynchronizer(databaseManager *DatabaseManager) *DatabaseWalSynchronizer {
	return &DatabaseWalSynchronizer{
		databaseManager: databaseManager,
	}
}

func (d *DatabaseWalSynchronizer) WriteAt(
	databaseId, branchId string,
	p []byte,
	off, sequence, timestamp int64,
) error {
	wal, err := d.databaseManager.Resources(databaseId, branchId).WalFile()

	if err != nil {
		log.Println(err)
		return err
	}

	_, err = wal.WriteAt(p, off, sequence, timestamp)

	if err != nil {
		log.Println(err)
		return err
	}

	return nil
}

func (d *DatabaseWalSynchronizer) Truncate(databaseId, branchId string, size, sequence, timestamp int64) error {
	wal, err := d.databaseManager.Resources(databaseId, branchId).WalFile()

	if err != nil {
		log.Println(err)
		return err
	}

	err = wal.Truncate(size, sequence, timestamp)

	if err != nil {
		log.Println(err)
		return err
	}

	return nil
}
