package backups

import (
	"fmt"
	"litebasedb/server/file"
	"litebasedb/server/storage"
	"log"
	"strconv"
	"strings"
	"time"
)

type RestorePoint struct {
	DatabaseUuid string
	BranchUuid   string
	Timestamp    int
}

// func ClearRestorePoints(databaseUuid string, branchUuid string, snapshotTimestamp int) {
// 	// Clear all restore points in the restore points directory.
// 	restorePointsDirectory := strings.Join([]string{
// 		file.GetFileDir(databaseUuid, branchUuid),
// 		RESTORE_POINTS_DIR,
// 	}, "/")

// 	directories, err := storage.FS().ReadDir(restorePointsDirectory)

// 	if err != nil {
// 		log.Fatal(err)
// 	}

// 	for _, directory := range directories {
// 		if directory.IsDir() {

// 			timestamp, err := strconv.Atoi(directory.Name())

// 			if err != nil {
// 				continue
// 			}

// 			if timestamp < snapshotTimestamp {
// 				storage.FS().RemoveAll(fmt.Sprintf("%s/%d", restorePointsDirectory,  directory.Name())"))
// 			}

// 		}
// 	}
// }

func GetRestorePoint(databaseUuid string, branchUuid string, timestamp int) *RestorePoint {
	return &RestorePoint{
		DatabaseUuid: databaseUuid,
		BranchUuid:   branchUuid,
		Timestamp:    timestamp,
	}
}

func SaveRestorePoint(databaseUuid string, branchUuid string, changedPages map[int][]byte) {
	snapshot := GetSnapShot(databaseUuid, branchUuid, int(time.Now().UTC().Unix()))

	for key, data := range changedPages {
		snapshot.AddPage(key, data)
	}
}

func (r *RestorePoint) Apply() {
	path, err := file.GetFilePath(r.DatabaseUuid, r.BranchUuid)

	if err != nil {
		log.Fatal(err)
	}

	databaseFile, err := storage.NewDatabaseFile(path)

	if err != nil {
		log.Fatal(err)
	}

	files, err := storage.FS().ReadDir(r.GetPath(r.DatabaseUuid, r.BranchUuid, r.Timestamp))

	if err != nil {
		log.Fatal(err)
	}

	for _, f := range files {
		if !f.IsDir() {
			pageNumber, err := strconv.Atoi(f.Name())

			if err != nil {
				continue
			}

			data, err := storage.FS().ReadFile(fmt.Sprintf("%s/%s", r.GetPath(r.DatabaseUuid, r.BranchUuid, r.Timestamp), f.Name()))

			if err != nil {
				continue
			}

			databasePage := &storage.DatabasePage{
				Data: data,
			}

			databaseFile.WritePage(pageNumber, databasePage)
		}
	}
}

func (r *RestorePoint) GetPath(databaseUuid string, branchUuid string, timestamp int) string {
	return strings.Join([]string{
		file.GetFileDir(databaseUuid, branchUuid),
		RESTORE_POINTS_DIR,
		fmt.Sprintf("%d", timestamp),
	}, "/")
}
