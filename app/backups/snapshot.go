package backups

import (
	"fmt"
	"litebasedb/runtime/app/file"
	"os"
	"strings"
)

type Snapshot struct {
	branchUuid   string
	databaseUuid string
	timestamp    int
}

func NewSnapshot(databaseUuid string, branchUuid string, timestamp int) *Snapshot {
	snapshot := &Snapshot{
		branchUuid:   branchUuid,
		databaseUuid: databaseUuid,
		timestamp:    timestamp,
	}

	return snapshot
}

func (s *Snapshot) AddPage(pageNumber int, data []byte) *Snapshot {
	path := fmt.Sprintf("%s/%d", s.GetPath(s.databaseUuid, s.branchUuid, s.timestamp), pageNumber)

	err := os.WriteFile(path, data, 0644)

	if err != nil {
		panic(err)
	}

	return s
}

func (s *Snapshot) GetPath(databaseUuid string, branchUuid string, timestamp int) string {
	return strings.Join([]string{
		file.GetFileDir(databaseUuid, branchUuid),
		BACKUP_DIR,
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

func (s *Snapshot) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"branchUuid":   s.branchUuid,
		"databaseUuid": s.databaseUuid,
		"timestamp":    s.timestamp,
	}
}
