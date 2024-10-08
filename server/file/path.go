package file

import (
	"crypto/sha1"
	"fmt"
	"litebase/internal/config"
	"path/filepath"
)

func DatabaseDirectory() string {
	return "_databases/"
}

func DatabaseTmpDirectory() string {
	return fmt.Sprintf("%s/_databases/", config.Get().TmpPath)
}

func DatabaseHash(
	databaseId string,
	branchId string,
) string {
	sha1 := sha1.New()
	sha1.Write([]byte(databaseId))
	sha1.Write([]byte(":"))
	sha1.Write([]byte(branchId))

	return fmt.Sprintf("%x", sha1.Sum(nil))
}

func DatabaseHashWithTimestamp(
	databaseId string,
	branchId string,
	timestamp int64,
) string {
	sha1 := sha1.New()
	sha1.Write([]byte(fmt.Sprintf("%s:%s:%d", databaseId, branchId, timestamp)))

	return fmt.Sprintf("%x", sha1.Sum(nil))
}

func GetDatabaseBackupsDirectory(databaseId, branchId string) string {
	return fmt.Sprintf(
		"%s/backups",
		GetDatabaseFileBaseDir(databaseId, branchId),
	)
}

func GetDatabaseFileBaseDir(databaseId string, branchId string) string {
	dir, err := GetDatabaseFilePath(databaseId, branchId)

	if err != nil {
		return ""
	}

	return filepath.Dir(dir)
}

func GetDatabaseFileDir(databaseId string, branchId string) string {
	return fmt.Sprintf(
		"%s%s/%s/%s",
		DatabaseDirectory(),
		databaseId,
		branchId,
		DatabaseHash(databaseId, branchId),
	)
}

func GetDatabaseFilePath(databaseId string, branchId string) (string, error) {
	return fmt.Sprintf(
		"%s%s/%s/%s.db",
		DatabaseDirectory(),
		databaseId,
		branchId,
		DatabaseHash(databaseId, branchId),
	), nil
}

func GetDatabaseFileTmpPath(nodeId, databaseId string, branchId string) (string, error) {
	return fmt.Sprintf(
		"%s%s/%s/%s/%s.db",
		DatabaseTmpDirectory(),
		nodeId,
		databaseId,
		branchId,
		DatabaseHash(databaseId, branchId),
	), nil
}

func GetDatabaseRollbackDirectory(databaseId, branchId string) string {
	return fmt.Sprintf(
		"%s/logs/rollback",
		GetDatabaseFileBaseDir(databaseId, branchId),
	)
}

func GetDatabaseSnapshotDirectory(databaseId, branchId string) string {
	return fmt.Sprintf(
		"%s/logs/snapshots",
		GetDatabaseFileBaseDir(databaseId, branchId),
	)
}
