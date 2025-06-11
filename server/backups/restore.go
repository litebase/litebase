package backups

import (
	"archive/tar"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/litebase/litebase/common/config"
	"github.com/litebase/litebase/server/file"
	"github.com/litebase/litebase/server/storage"
)

var ErrorRestoreBackupNotFound = errors.New("restore backup not found")

// OPTIMIZE: Use copy commands instead of reading and writing files
// Copying the source database to the target database requires the following:
// 1. Copy all of the range files
// 2. Copy the _METADATA file
// 3. Copy any page logs and their indexes
func CopySourceDatabaseToTargetDatabase(
	maxPageNumber int64,
	sourceDatabaseUuid,
	sourceBranchUuid,
	targetDatabaseUuid,
	targetBranchUuid string,
	sourceFileSystem *storage.DurableDatabaseFileSystem,
	targetFileSystem *storage.DurableDatabaseFileSystem,
) error {
	err := copySourceDatabaseRangeFilesToTargetDatabase(
		maxPageNumber,
		sourceDatabaseUuid,
		sourceBranchUuid,
		targetDatabaseUuid,
		targetBranchUuid,
		sourceFileSystem,
		targetFileSystem,
	)

	if err != nil {
		return err
	}

	// TODO: Need to prevent the source database from checkpointing from WAL
	// to page log while we are copying the files
	err = copySourceDatabasePageLogsToTargetDatabase(
		sourceDatabaseUuid,
		sourceBranchUuid,
		targetDatabaseUuid,
		targetBranchUuid,
		sourceFileSystem,
		targetFileSystem,
	)

	if err != nil {
		return err
	}

	err = targetFileSystem.Compact()

	if err != nil {
		log.Println("Error compacting target database:", err)

		return err
	}

	return nil
}

func copySourceDatabasePageLogsToTargetDatabase(
	sourceDatabaseUuid,
	sourceBranchUuid,
	targetDatabaseUuid,
	targetBranchUuid string,
	sourceFileSystem *storage.DurableDatabaseFileSystem,
	targetFileSystem *storage.DurableDatabaseFileSystem,
) error {
	sourceDirectory := fmt.Sprintf("%slogs/page/", file.GetDatabaseFileBaseDir(sourceDatabaseUuid, sourceBranchUuid))
	targetDirectory := fmt.Sprintf("%slogs/page/", file.GetDatabaseFileBaseDir(targetDatabaseUuid, targetBranchUuid))

	// Loop through the files in the source database and copy them to the target database
	entries, err := sourceFileSystem.PageLogger.NetworkFS.ReadDir(sourceDirectory)

	if err != nil {
		if os.IsNotExist(err) {
			return ErrorRestoreBackupNotFound
		}

		return err
	}

	for _, entry := range entries {
		sourceFilePath := fmt.Sprintf("%s%s", sourceDirectory, entry.Name())
		sourceFile, err := sourceFileSystem.PageLogger.NetworkFS.Open(sourceFilePath)

		if err != nil {
			log.Println("Error opening source file:", sourceFilePath, err)
			return err
		}

		err = targetFileSystem.PageLogger.NetworkFS.MkdirAll(targetDirectory, 0755)

		if err != nil {
			log.Println("Error creating target directory:", targetDirectory, err)

			return err
		}

		targetFilePath := fmt.Sprintf("%s%s", targetDirectory, entry.Name())

		targetFile, err := targetFileSystem.PageLogger.NetworkFS.Create(targetFilePath)

		if err != nil {
			log.Println("Error writing file:", entry.Name(), err)
			return err
		}

		_, err = io.Copy(targetFile, sourceFile)

		if err != nil {
			log.Println("Error copying page:", entry.Name(), err)
			return err
		}

		targetFile.Close()
		sourceFile.Close()
	}

	return nil
}

func copySourceDatabaseRangeFilesToTargetDatabase(
	maxPageNumber int64,
	sourceDatabaseUuid,
	sourceBranchUuid,
	targetDatabaseUuid,
	targetBranchUuid string,
	sourceFileSystem *storage.DurableDatabaseFileSystem,
	targetFileSystem *storage.DurableDatabaseFileSystem,
) error {
	maxRangeNumber := file.PageRange(maxPageNumber, storage.RangeMaxPages)
	sourceDirectory := file.GetDatabaseFileDir(sourceDatabaseUuid, sourceBranchUuid)
	targetDirectory := file.GetDatabaseFileDir(targetDatabaseUuid, targetBranchUuid)

	// Loop through the files in the source database and copy them to the target database
	entries, err := sourceFileSystem.FileSystem().ReadDir(sourceDirectory)

	if err != nil {
		if os.IsNotExist(err) {
			return ErrorRestoreBackupNotFound
		}

		slog.Error("Error reading source directory", "directory", sourceDirectory, "error", err)

		// If the source directory does not exist, we cannot restore the backup
		return err
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		if entry.Name()[0] == '_' {
			continue
		}

		rangeNumber, err := strconv.ParseInt(entry.Name(), 10, 64)

		if err != nil {
			log.Println("Error parsing entry name:", entry.Name(), err)
			return err
		}

		if rangeNumber > maxRangeNumber {
			continue
		}

		// Copy the file from the source to the target
		sourceFilePath := fmt.Sprintf("%s%s", sourceDirectory, entry.Name())

		sourceFile, err := sourceFileSystem.FileSystem().Open(sourceFilePath)

		if err != nil {
			log.Println("Error opening source file:", sourceFilePath, err)
			return err
		}

		targetFilePath := fmt.Sprintf("%s%s", targetDirectory, entry.Name())

		targetFile, err := targetFileSystem.FileSystem().Create(targetFilePath)

		if err != nil {
			log.Println("Error writing file:", rangeNumber, err)
			return err
		}

		_, err = io.Copy(targetFile, sourceFile)

		if err != nil {
			log.Println("Error copying page:", rangeNumber, err)
			return err
		}

		targetFile.Close()
		sourceFile.Close()
	}

	return nil
}

func RestoreFromBackup(
	timestamp int64,
	sourceDatabaseUuid string,
	sourceBranchUuid string,
	targetDatabaseUuid string,
	targetBranchUuid string,
	sourceFileSystem *storage.DurableDatabaseFileSystem,
	targetFileSystem *storage.DurableDatabaseFileSystem,
) error {
	// Check if the souce database file system has the files for the specified timestamp
	sourceDatabasePath := file.GetDatabaseBackupsDirectory(sourceDatabaseUuid, sourceBranchUuid)
	timestampPath := fmt.Sprintf("%s%d", sourceDatabasePath, timestamp)
	backupParts := []string{}

	entries, err := sourceFileSystem.FileSystem().ReadDir(timestampPath)

	if err != nil {
		if os.IsNotExist(err) {
			return ErrorRestoreBackupNotFound
		}

		return err
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		// Since only one backup exists per directory, all files are backup files
		if strings.HasPrefix(entry.Name(), "backup-") && strings.HasSuffix(entry.Name(), ".tar.gz") {
			backupParts = append(backupParts, entry.Name())
		}
	}

	if len(backupParts) == 0 {
		log.Println("Backup not found for the specified timestamp")
		return errors.New("backup not found for the specified timestamp")
	}

	// Order the backup parts by the suffix
	sort.Slice(backupParts, func(i, j int) bool {
		// Extract numeric suffixes
		getSuffix := func(filename string) int {
			parts := strings.Split(filename, "_")

			if len(parts) < 2 {
				return 0
			}

			suffix := strings.TrimSuffix(parts[len(parts)-1], ".tar.gz")

			num, err := strconv.Atoi(suffix)

			if err != nil {
				return 0
			}

			return num
		}

		return getSuffix(backupParts[i]) < getSuffix(backupParts[j])
	})

	// OPTIMIZE: We can do this with parallelism
	// Open each tar.gz backup part and write it to the target database
	for _, backupPart := range backupParts {
		backupPartPath := fmt.Sprintf("%s/%s", timestampPath, backupPart)
		backupFile, err := sourceFileSystem.FileSystem().OpenFile(backupPartPath, os.O_RDWR, 0644)

		if err != nil {
			log.Println("Error opening backup part:", backupPartPath)

			return err
		}

		backupFile.Seek(0, io.SeekStart) // Ensure we start reading from the beginning

		gzipReader, err := gzip.NewReader(backupFile)

		if err != nil {
			log.Println("Error creating gzip reader:", err)

			return err
		}

		tarReader := tar.NewReader(gzipReader)

		for {
			header, err := tarReader.Next()

			if errors.Is(err, io.EOF) {
				break
			}

			if err != nil {
				log.Println("Error reading tar header:", err)

				return err
			}

			switch header.Typeflag {
			case tar.TypeDir:
				// The database directory should only contain files
			case tar.TypeReg:
				data, err := io.ReadAll(tarReader)

				if err != nil {
					log.Println("Error reading file data:", header.Name, err)
				}

				err = targetFileSystem.FileSystem().WriteFile(
					file.GetDatabaseFileDir(targetDatabaseUuid, targetBranchUuid)+header.Name,
					data,
					0644,
				)

				if err != nil {
					log.Println("Error writing file:", header.Name, err)

					return err
				}

				if header.Name == "_METADATA" {
					// It is important to reload the metadata after writing to it
					targetFileSystem.Metadata().Load()
				}
			}
		}
	}

	return nil
}

func RestoreFromTimestamp(
	c *config.Config,
	tieredFS *storage.FileSystem,
	sourceDatabaseUuid string,
	sourceBranchUuid string,
	targetDatabaseUuid string,
	targetBranchUuid string,
	backupTimestamp int64,
	snapshotLogger *SnapshotLogger,
	sourceFileSystem *storage.DurableDatabaseFileSystem,
	targetFileSystem *storage.DurableDatabaseFileSystem,
	onComplete func(func() error) error,
) error {
	// Truncate the timestamp to the start of the hour
	startOfHourTimestamp := time.Unix(0, backupTimestamp).Truncate(time.Hour).UnixNano()
	rollbackLogger := NewRollbackLogger(tieredFS, sourceDatabaseUuid, sourceBranchUuid)

	snapshot, err := snapshotLogger.GetSnapshot(backupTimestamp)

	if err != nil {
		return err
	}

	restorePoint, err := snapshot.GetRestorePoint(backupTimestamp)

	if err != nil {
		return err
	}

	// Walk the files in the rollback logs directory
	directory := file.GetDatabaseRollbackDirectory(sourceDatabaseUuid, sourceBranchUuid)

	entries, err := tieredFS.ReadDir(directory)

	if err != nil {
		return err
	}

	rollbackLogTimestamps := make([]int64, 0)

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		entryTimeNano, err := strconv.ParseInt(entry.Name(), 10, 64) // Convert string to int64

		if err != nil {
			log.Println("Error parsing entry name as timestamp:", entry.Name(), err)
			return err
		}

		entryTime := time.Unix(0, entryTimeNano) // Convert UnixNano to time.Time
		entryTimestamp := entryTime.UnixNano()

		if startOfHourTimestamp < entryTimestamp {
			continue
		}

		rollbackLogTimestamps = append(rollbackLogTimestamps, entryTimestamp)
	}

	if len(rollbackLogTimestamps) == 0 {
		log.Println("No rollback logs found for the specified timestamp")
		return nil
	}

	// Sort the timestamps in descending order
	sort.Slice(rollbackLogTimestamps, func(i, j int) bool {
		return rollbackLogTimestamps[i] > rollbackLogTimestamps[j]
	})

	// Copy the source database files to the target database
	err = CopySourceDatabaseToTargetDatabase(
		restorePoint.PageCount,
		sourceDatabaseUuid,
		sourceBranchUuid,
		targetDatabaseUuid,
		targetBranchUuid,
		sourceFileSystem,
		targetFileSystem,
	)

	if err != nil {
		slog.Error("Error copying source database to target database", "error", err)
		return err
	}

	// Open the rollback logs and restore the pages
	for _, entryTimestamp := range rollbackLogTimestamps {
		rollbackLog, err := rollbackLogger.GetLog(entryTimestamp)

		if err != nil {
			log.Println("Error opening rollback log:", err)
			return err
		}

		rollbackLogEntries, doneChannel, errorChannel := rollbackLog.ReadForTimestamp(backupTimestamp)

	rollbackLogTimestampsLoop:
		for {
			select {
			case <-doneChannel:
				break rollbackLogTimestampsLoop
			case err := <-errorChannel:
				log.Println("Error reading rollback log:", err)
				return err
			case frame := <-rollbackLogEntries:
				for _, rollbackLogEntry := range frame {
					// Only apply rollback logs for pages within the restore point's page count
					if rollbackLogEntry.PageNumber > restorePoint.PageCount {
						continue
					}

					err := targetFileSystem.WriteToRange(
						rollbackLogEntry.PageNumber,
						rollbackLogEntry.Data,
					)

					if err != nil {
						log.Println("Error writing page:", rollbackLogEntry.PageNumber, err)
						return err
					}
				}
			}
		}
	}

	// Truncate the database file
	err = targetFileSystem.Truncate(int64(restorePoint.PageCount) * c.PageSize)

	if err != nil {
		log.Println("Error truncating database file:", err)
		return err
	}

	err = targetFileSystem.Metadata().SetPageCount(restorePoint.PageCount)

	if err != nil {
		log.Println("Error setting page count:", err)
		return err
	}

	// Wrap things up after running this callback
	return onComplete(func() error {
		return nil
	})
}
