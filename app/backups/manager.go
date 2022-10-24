package backups

import (
	"os"
)

type BackupManager struct{}

var BackupManagerInstance *BackupManager

func Manager() *BackupManager {
	if BackupManagerInstance == nil {
		BackupManagerInstance = &BackupManager{}
	}

	return BackupManagerInstance
}

// func (m *BackupManager) Handle(changedPages []int) {
// 	currentTime := time.Now()
// 	tmpPath := config.Get("tmp_path")
// 	path := fmt.Sprintf("%s/litebasedb/ibm-%d", tmpPath, currentTime.Unix())

// 	if !m.shouldBackup(path) {
// 		return
// 	}

// 	// Check if the temporary directory exists
// 	if _, err := os.Stat(tmpPath); os.IsNotExist(err) {
// 		os.MkdirAll(tmpPath, 0755)
// 	}

// 	// Check if the LitebaseDB directory exists
// 	if _, err := os.Stat(tmpPath + "/litebasedb"); os.IsNotExist(err) {
// 		os.MkdirAll(tmpPath+"/litebasedb", 0755)
// 	}

// 	// Create the backup
// 	os.WriteFile(path, []byte(""), 0644)

// 	_, err := RunIncrementalBackup(config.Get("database_uuid"), config.Get("branch_uuid"), changedPages)

// 	if err != nil {
// 		os.Remove(path)
// 		log.Fatal(err)
// 	}
// }

func (m *BackupManager) shouldBackup(path string) bool {
	_, err := os.Stat(path)

	if err != nil && os.IsNotExist(err) {
		return true
	}

	return false
}
