package file

import (
	"crypto/sha256"
	"fmt"
	"path/filepath"

	"github.com/litebase/litebase/pkg/config"
)

func DatabaseDirectory() string {
	return "_databases/"
}

func DatabaseHash(
	databaseId string,
	branchId string,
) string {
	hash := sha256.New()
	hash.Write([]byte(databaseId))
	hash.Write([]byte(":"))
	hash.Write([]byte(branchId))

	return fmt.Sprintf("%x", hash.Sum(nil))
}

func GetDatabaseBackupsDirectory(databaseId, branchId string) string {
	return fmt.Sprintf(
		"%sbackups/",
		GetDatabaseFileBaseDir(databaseId, branchId),
	)
}

// Return the root directoy of a branch
func GetDatabaseBranchRootDir(databaseID, branchID string) string {
	return fmt.Sprintf(
		"%s%s/%s/",
		DatabaseDirectory(),
		databaseID,
		branchID,
	)
}

func GetDatabaseFileBaseDir(databaseId string, branchId string) string {
	dir, err := GetDatabaseFilePath(databaseId, branchId)

	if err != nil {
		return ""
	}

	return filepath.Dir(dir) + "/"
}

func GetDatabaseFileDir(databaseId string, branchId string) string {
	return fmt.Sprintf(
		"%s%s/%s/%s/",
		DatabaseDirectory(),
		databaseId,
		branchId,
		DatabaseHash(databaseId, branchId),
	)
}

func GetDatabaseRootDir(databaseId string) string {
	return fmt.Sprintf(
		"%s%s/",
		DatabaseDirectory(),
		databaseId,
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

func GetDatabaseFileTmpPath(c *config.Config, nodeId, databaseId string, branchId string) (string, error) {
	return fmt.Sprintf(
		"%s/%s/_databases/%s/%s/%s.db",
		c.TmpPath,
		nodeId,
		databaseId,
		branchId,
		DatabaseHash(databaseId, branchId),
	), nil
}

func GetDatabaseFileTmpWALPath(c *config.Config, nodeId, databaseId string, branchId string) (string, error) {
	return fmt.Sprintf(
		"%s/%s/%s",
		c.TmpPath,
		nodeId,
		WALPath(databaseId, branchId),
	), nil
}

func GetDatabaseRollbackDirectory(databaseId, branchId string) string {
	return fmt.Sprintf(
		"%slogs/rollback",
		GetDatabaseFileBaseDir(databaseId, branchId),
	)
}

func GetDatabaseSnapshotDirectory(databaseId, branchId string) string {
	return fmt.Sprintf(
		"%slogs/snapshots",
		GetDatabaseFileBaseDir(databaseId, branchId),
	)
}
func WALPath(databaseId, branchId string) string {
	return fmt.Sprintf(
		"%s%s/%s/%s.db-wal",
		DatabaseDirectory(),
		databaseId,
		branchId,
		DatabaseHash(databaseId, branchId),
	)
}
