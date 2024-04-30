package backups

import (
	"bufio"
	"compress/gzip"
	"crypto/sha1"
	"fmt"
	"io"
	"litebasedb/internal/config"
	"litebasedb/server/file"
	"litebasedb/server/storage"
	"sort"
	"strconv"

	"log"
	"os"
	"path/filepath"
	"time"
)

type Backup struct {
	databaseUuid      string
	branchUuid        string
	SnapshotTimestamp int
}

func GetBackup(databaseUuid string, branchUuid string, snapshotTimestamp time.Time) *Backup {
	backup := &Backup{
		databaseUuid:      databaseUuid,
		branchUuid:        branchUuid,
		SnapshotTimestamp: int(snapshotTimestamp.UTC().Unix()),
	}

	return backup
}

func GetNextBackup(databaseUuid string, branchUuid string, snapshotTimestamp int) *Backup {
	backups := make([]int, 0)
	backupsDirectory := fmt.Sprintf("%s/%s", file.GetFileDir(databaseUuid, branchUuid), BACKUP_DIR)

	// Get a list of all directories in the directory
	dirs, err := storage.FS().ReadDir(backupsDirectory)

	if err != nil {
		log.Fatal(err)
	}

	// Loop through the directories
	for _, dir := range dirs {
		// Get the timestamp of the directory

		if !dir.IsDir() {
			continue
		}

		timestamp, err := strconv.Atoi(dir.Name())

		if err != nil {
			log.Fatal(err)
		}

		// If the timestamp is greater than the current timestamp, then return the backup
		backups = append(backups, timestamp)
	}

	// Sort the backups
	sort.Ints(backups)

	// Loop through the backups
	for _, b := range backups {
		if b > snapshotTimestamp {
			return GetBackup(databaseUuid, branchUuid, time.Unix(int64(b), 0))
		}
	}

	return nil
}

func (backup *Backup) BackupKey() string {
	hash := sha1.New()
	hash.Write([]byte(fmt.Sprintf("%s-%s-%d", backup.databaseUuid, backup.branchUuid, backup.SnapshotTimestamp)))

	return fmt.Sprintf("%s/%d/%x.db.gz", BACKUP_DIR, backup.SnapshotTimestamp, hash.Sum(nil))
}

func (backup *Backup) Delete() {
	backup.deleteFile()
	backup.deleteArchiveFile()
}

func (backup *Backup) deleteArchiveFile() {
	if config.Get().Env == "local" {
		storageDir := file.GetFileDir(backup.databaseUuid, backup.branchUuid)
		storage.FS().Remove(fmt.Sprintf("%s/archives/%s", storageDir, backup.BackupKey()))

		return
	}

	// TODO: Update
}

func (backup *Backup) deleteFile() {
	if _, err := storage.FS().Stat(backup.Path()); os.IsNotExist(err) {
		return
	}

	storage.FS().Remove(backup.Path())
}

func (backup *Backup) packageBackup() string {
	input, err := file.GetFilePath(backup.databaseUuid, backup.branchUuid)

	if err != nil {
		log.Fatal(err)
	}

	output := backup.Path()

	file, err := storage.FS().Open(input)

	if err != nil {
		log.Fatal(err)
	}

	defer file.Close()

	reader := bufio.NewReader(file)

	err = storage.FS().MkdirAll(filepath.Dir(output), 0755)

	if err != nil {
		log.Fatal(err)
	}

	gzipFile, err := storage.FS().Create(output)

	if err != nil {
		log.Fatal(err)
	}

	defer gzipFile.Close()
	writer := gzip.NewWriter(gzipFile)

	defer writer.Close()

	_, err = io.Copy(writer, reader)

	if err != nil {
		log.Fatal(err)
	}

	return output
}

func (backup *Backup) Path() string {
	return fmt.Sprintf("%s/%s", file.GetFileDir(backup.databaseUuid, backup.branchUuid), backup.BackupKey())
}

func RunBackup(databaseUuid string, branchUuid string) (*Backup, error) {
	backup := &Backup{
		branchUuid:   branchUuid,
		databaseUuid: databaseUuid,
	}

	backup.SnapshotTimestamp = int(time.Now().UTC().Unix())

	backup.packageBackup()

	return backup, nil
}

func (backup *Backup) Size() int64 {
	stat, err := storage.FS().Stat(backup.Path())

	if err != nil {
		return 0
	}

	return stat.Size()
}

func (backup *Backup) ToMap() map[string]interface{} {
	return map[string]interface{}{
		"databaseUuid": backup.databaseUuid,
		"branchUuid":   backup.branchUuid,
		"size":         backup.Size(),
		"timestamp":    backup.SnapshotTimestamp,
	}
}

func (backup *Backup) Upload() map[string]interface{} {
	if config.Get().Env == "testing" {
		return map[string]interface{}{
			"key":  "test",
			"size": 0,
		}
	}

	path := backup.packageBackup()
	key := filepath.Base(path)

	if _, err := storage.FS().Stat(path); os.IsNotExist(err) {
		log.Fatalf("Backup archive file not found: %s", path)
	}

	if config.Get().Env == "local" {
		storageDir := file.GetFileDir(backup.databaseUuid, backup.branchUuid)

		if _, err := storage.FS().Stat(storageDir); os.IsNotExist(err) {
			storage.FS().Mkdir(storageDir, 0755)
		}

		source, err := storage.FS().ReadFile(path)

		if err != nil {
			log.Fatal(err)
		}

		storage.FS().WriteFile(fmt.Sprintf("%s/%s", storageDir, key), source, 0666)
	} else {

	}

	stat, err := storage.FS().Stat(path)

	if err != nil {
		log.Fatal(err)
	}

	return map[string]interface{}{
		"key":  key,
		"size": stat.Size(),
	}
}
