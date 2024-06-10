package file

import (
	"crypto/sha1"
	"fmt"
	"litebasedb/internal/config"
	"path/filepath"
)

func DatabaseDirectory() string {
	return fmt.Sprintf("%s/.litebasedb/_databases", config.Get().DataPath)
}

func DatabaseHash(
	databaseUuid string,
	branchUuid string,
) string {
	sha1 := sha1.New()
	sha1.Write([]byte(fmt.Sprintf("%s:%s", databaseUuid, branchUuid)))

	return fmt.Sprintf("%x", sha1.Sum(nil))
}

func GetDatabaseFileDir(databaseUuid string, branchUuid string) string {
	dir, err := GetDatabaseFilePath(databaseUuid, branchUuid)

	if err != nil {
		return ""
	}

	return filepath.Dir(dir)
}

func GetDatabaseFilePath(databaseUuid string, branchUuid string) (string, error) {
	return fmt.Sprintf(
		"%s/%s/%s/%s.db",
		DatabaseDirectory(),
		databaseUuid,
		branchUuid,
		DatabaseHash(databaseUuid, branchUuid),
	), nil
}
