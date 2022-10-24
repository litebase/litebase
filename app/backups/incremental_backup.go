package backups

import (
	"time"
)

type IncrementalBackup struct {
}

func RunIncrementalBackup(databaseUuid string, branchUuid string, changedPages map[int][]byte) {
	snapshot := GetSnapShot(databaseUuid, branchUuid, int(time.Now().UTC().Unix()))

	for key, data := range changedPages {
		snapshot.AddPage(key, data)
	}
}
