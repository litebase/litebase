package backups

import (
	"fmt"
	"litebasedb/runtime/app/auth"
	"path/filepath"
	"strings"
)

type AccessHeadFile struct{}

func (a *AccessHeadFile) headFilePath(databaseUuid string, branchUuid string, snapshotTimestamp int64) string {
	path, err := auth.SecretsManager().GetPath(databaseUuid, branchUuid)

	if err != nil {
		return ""
	}

	databaseDirectory := filepath.Dir(path)

	return strings.Join([]string{
		databaseDirectory,
		BACKUP_DIR,
		fmt.Sprintf("%x", snapshotTimestamp),
		"head",
	}, "/")
}
