package backups

import (
	"fmt"
	"litebasedb/runtime/app/database"
	"strings"
)

type AccessHeadFile struct{}

func (a *AccessHeadFile) headFilePath(databaseUuid string, branchUuid string, snapshotTimestamp int64) string {
	return strings.Join([]string{
		database.GetFileDir(databaseUuid, branchUuid),
		BACKUP_DIR,
		fmt.Sprintf("%x", snapshotTimestamp),
		"head",
	}, "/")
}
