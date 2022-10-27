package backups

import (
	"compress/gzip"
	"io"
	"litebasedb/runtime/file"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

func RestoreFromDatabaseBackup(databaseUuid string, branchUuid string, backupTimestamp int) error {
	timestamp := time.Unix(int64(backupTimestamp), 0)
	backup := GetBackup(databaseUuid, branchUuid, timestamp)
	source, err := os.Open(backup.Path())

	if err != nil {
		return err
	}

	defer source.Close()

	destination, err := file.GetFilePath(databaseUuid, branchUuid)
	filePath := destination + ".restore"

	if err != nil {
		return err
	}

	archive, err := gzip.NewReader(source)

	if err != nil {
		return err
	}

	defer archive.Close()

	destinationFile, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)

	if err != nil {
		return err
	}

	defer destinationFile.Close()

	_, err = io.Copy(destinationFile, archive)

	if err != nil {
		return err
	}

	// Rename the source file to the destination file.
	err = os.Rename(destination, destination+".bak")

	if err != nil {
		return err
	}

	err = os.Rename(filePath, destination)

	if err != nil {
		return err
	}

	// Delet ehte backup file.
	err = os.Remove(destination + ".bak")

	if err != nil {
		return err
	}

	return nil
}

func RestoreFromDatabaseBackupAtPointInTime(databaseUuid string, branchUuid string, backupTimestamp int, restorePointTimestamp int) error {
	RestoreFromDatabaseBackup(databaseUuid, branchUuid, backupTimestamp)

	restorePointsDirectory := strings.Join([]string{
		file.GetFileDir(databaseUuid, branchUuid),
		RESTORE_POINTS_DIR,
	}, "/")

	directories, err := os.ReadDir(restorePointsDirectory)
	if err != nil {
		return err
	}

	timestamps := make([]int, 0)

	// Apply the restore points to the database. after the backup has been restored.
	for _, directory := range directories {
		if directory.IsDir() {
			timestamp, err := strconv.Atoi(directory.Name())

			if err != nil {
				continue
			}

			timestamps = append(timestamps, timestamp)
		}
	}

	sort.Ints(timestamps)

	for _, timestamp := range timestamps {
		if timestamp <= restorePointTimestamp {
			restorePoint := GetRestorePoint(databaseUuid, branchUuid, timestamp)
			restorePoint.Apply()
		}
	}

	return nil
}
