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
	databaseUuid string,
	branchUuid string,
) string {
	sha1 := sha1.New()
	sha1.Write([]byte(databaseUuid))
	sha1.Write([]byte(":"))
	sha1.Write([]byte(branchUuid))

	return fmt.Sprintf("%x", sha1.Sum(nil))
}

func DatabaseHashWithTimestamp(
	databaseUuid string,
	branchUuid string,
	timestamp int64,
) string {
	sha1 := sha1.New()
	sha1.Write([]byte(fmt.Sprintf("%s:%s:%d", databaseUuid, branchUuid, timestamp)))

	return fmt.Sprintf("%x", sha1.Sum(nil))
}

func GetDatabaseBackupsDirectory(databaseUuid, branchUuid string) string {
	return fmt.Sprintf(
		"%s/backups",
		GetDatabaseFileBaseDir(databaseUuid, branchUuid),
	)
}

func GetDatabaseFileBaseDir(databaseUuid string, branchUuid string) string {
	dir, err := GetDatabaseFilePath(databaseUuid, branchUuid)

	if err != nil {
		return ""
	}

	return filepath.Dir(dir)
}

func GetDatabaseFileDir(databaseUuid string, branchUuid string) string {
	return fmt.Sprintf(
		"%s%s/%s/%s",
		DatabaseDirectory(),
		databaseUuid,
		branchUuid,
		DatabaseHash(databaseUuid, branchUuid),
	)
}

func GetDatabaseFilePath(databaseUuid string, branchUuid string) (string, error) {
	return fmt.Sprintf(
		"%s%s/%s/%s.db",
		DatabaseDirectory(),
		databaseUuid,
		branchUuid,
		DatabaseHash(databaseUuid, branchUuid),
	), nil
}

func GetDatabaseFileTmpPath(nodeId, databaseUuid string, branchUuid string) (string, error) {
	return fmt.Sprintf(
		"%s%s/%s/%s/%s.db",
		DatabaseTmpDirectory(),
		nodeId,
		databaseUuid,
		branchUuid,
		DatabaseHash(databaseUuid, branchUuid),
	), nil
}

func GetDatabaseRollbackDirectory(databaseUuid, branchUuid string) string {
	return fmt.Sprintf(
		"%s/logs/rollback",
		GetDatabaseFileBaseDir(databaseUuid, branchUuid),
	)
}
