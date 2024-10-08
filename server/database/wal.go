package database

import (
	"fmt"
	"litebase/server/file"
	"litebase/server/storage"
)

func CreateWalVersion(databaseId, branchId string, timestamp int64) error {
	walVersionFile, err := storage.LocalFS().Create(WalVersionPath(databaseId, branchId, timestamp))

	if err != nil {
		return err
	}

	defer walVersionFile.Close()

	return nil
}

func WalPath(databaseId, branchId string) string {
	return fmt.Sprintf(
		"%s/%s.db-wal",
		Resources(databaseId, branchId).
			TempFileSystem().
			Path(),
		file.DatabaseHash(databaseId, branchId),
	)
}

func WalVersionPath(databaseId, branchId string, timestamp int64) string {
	if timestamp == 0 {
		return WalPath(databaseId, branchId)
	}

	return fmt.Sprintf(
		"%s/%s.db-wal_%d",
		Resources(databaseId, branchId).
			TempFileSystem().
			Path(),
		file.DatabaseHash(databaseId, branchId),
		timestamp,
	)
}
