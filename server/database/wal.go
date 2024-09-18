package database

import (
	"fmt"
	"litebase/server/file"
	"litebase/server/storage"
)

func CreateWalVersion(databaseUuid, branchUuid string, timestamp int64) error {
	walVersionFile, err := storage.LocalFS().Create(WalVersionPath(databaseUuid, branchUuid, timestamp))

	if err != nil {
		return err
	}

	defer walVersionFile.Close()

	return nil
}

func WalPath(databaseUuid, branchUuid string) string {
	return fmt.Sprintf(
		"%s/%s.db-wal",
		Resources(databaseUuid, branchUuid).
			TempFileSystem().
			Path(),
		file.DatabaseHash(databaseUuid, branchUuid),
	)
}

func WalVersionPath(databaseUuid, branchUuid string, timestamp int64) string {
	if timestamp == 0 {
		return WalPath(databaseUuid, branchUuid)
	}

	return fmt.Sprintf(
		"%s/%s.db-wal_%d",
		Resources(databaseUuid, branchUuid).
			TempFileSystem().
			Path(),
		file.DatabaseHash(databaseUuid, branchUuid),
		timestamp,
	)
}
