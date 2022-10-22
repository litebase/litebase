package backups

import (
	"litebasedb/runtime/app/file"
	"log"
	"sort"
	"time"
)

type IncrementalBackup struct {
	Backup
}

func RunIncrementalBackup(databaseUuid string, branchUuid string, changePages []int) (interface{}, error) {
	backup := &IncrementalBackup{
		Backup: Backup{
			branchUuid:   branchUuid,
			databaseUuid: databaseUuid,
			fileDirCache: make(map[string]bool),
			pageHashes:   make([]string, 0),
		},
	}

	backup.snapshotTimestamp = backup.Timestamp(time.Now())
	backup.snapshot = backup.GetSnapShot()

	if backup.snapshot == nil {
		return RunFullBackup(databaseUuid, branchUuid)
	}

	lock := backup.ObtainLock()

	if lock == nil {
		log.Fatal("Cannot run an incremental backup while another backup is running.")
	}

	if len(changePages) == 0 {
		return nil, nil
	}

	dir, err := file.GetFilePath(databaseUuid, branchUuid)

	if err != nil {
		return nil, err
	}

	databaseFile, err := file.NewDatabaseFile(dir)

	if err != nil {
		return nil, err
	}

	defer databaseFile.Close()

	sort.Ints(changePages)

	for _, page := range changePages {
		pageData := databaseFile.ReadPage(page).Data
		backup.pageHashes = append(backup.pageHashes, backup.writePage(pageData))
	}

	if len(backup.pageHashes) >= 1 {
		backup.snapshot.AddCommits([]*Commit{
			NewCommit(
				databaseUuid,
				branchUuid,
				backup.snapshotTimestamp,
				backup.Timestamp(time.Now()),
				"",
				backup.pageHashes,
			),
		})
	}

	lock.Release()

	return backup, nil
}
