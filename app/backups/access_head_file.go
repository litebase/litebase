package backups

import (
	"fmt"
	"litebasedb/runtime/app/file"
	"strings"
)

type AccessHeadFile struct{}

func (a *AccessHeadFile) headFilePath(databaseUuid string, branchUuid string, snapshotTimestamp int) string {
	return strings.Join([]string{
		file.GetFileDir(databaseUuid, branchUuid),
		BACKUP_DIR,
		fmt.Sprintf("%d", snapshotTimestamp),
		"head",
	}, "/")
}
