package backups

import (
	"fmt"
	"litebasedb/runtime/app/file"
	"os"
	"strconv"
	"strings"
	"time"
)

type Snapshot struct {
	BranchUuid    string `json:"branchUuid"`
	DatabaseUuid  string `json:"databaseUuid"`
	RestorePoints []int  `json:"restorePoints"`
	Timestamp     int    `json:"timestamp"`
}

func NewSnapshot(databaseUuid string, branchUuid string, timestamp int) *Snapshot {
	snapshot := &Snapshot{
		BranchUuid:   branchUuid,
		DatabaseUuid: databaseUuid,
		Timestamp:    timestamp,
	}

	return snapshot
}

func (s *Snapshot) AddPage(pageNumber int, data []byte) *Snapshot {
	path := fmt.Sprintf("%s/%d", s.GetPath(s.DatabaseUuid, s.BranchUuid, s.Timestamp), pageNumber)

	err := os.WriteFile(path, data, 0644)

	if err != nil {
		panic(err)
	}

	return s
}

func (s *Snapshot) GetPath(databaseUuid string, branchUuid string, timestamp int) string {
	return strings.Join([]string{
		file.GetFileDir(databaseUuid, branchUuid),
		RESTORE_POINTS_DIR,
		fmt.Sprintf("%d", timestamp),
	}, "/")
}

func GetSnapShot(databaseUuid string, branchUuid string, timestamp int) *Snapshot {
	snapshot := NewSnapshot(databaseUuid, branchUuid, timestamp)
	path := snapshot.GetPath(databaseUuid, branchUuid, timestamp)

	if _, err := os.Stat(path); os.IsNotExist(err) {
		os.Mkdir(path, 0755)
	}

	return snapshot
}

func (s *Snapshot) WithRestorePoints() *Snapshot {
	// Get all the directories in the backup directory
	// and return the ones that are greater than the timestamp
	// of the snapshot
	backupDirectory := strings.Join([]string{
		file.GetFileDir(s.DatabaseUuid, s.BranchUuid),
		RESTORE_POINTS_DIR,
	}, "/")

	directories, err := os.ReadDir(backupDirectory)

	if err != nil {
		panic(err)
	}

	futureDate := int(time.Now().UTC().Add(time.Hour * 24 * 3).Unix())

	for _, directory := range directories {
		if directory.IsDir() {
			timestamp, err := strconv.Atoi(directory.Name())

			if err != nil {
				continue
			}

			if timestamp >= s.Timestamp && timestamp < futureDate {
				s.RestorePoints = append(s.RestorePoints, timestamp)
			}
		}
	}

	return s
}
