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

	"github.com/litebase/litebase/pkg/config"
	"github.com/litebase/litebase/pkg/file"
	"github.com/litebase/litebase/pkg/storage"
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
	checkpointer Checkpointer,
) error {
	// Prevent the page logger from writing to the source database range files
	// during this operation
	err := sourceFileSystem.PageLogger.CompactionBarrier(func() error {
		return copySourceDatabaseRangeFilesToTargetDatabase(
			maxPageNumber,
			sourceDatabaseUuid,
			sourceBranchUuid,
			targetDatabaseUuid,
			targetBranchUuid,
			sourceFileSystem,
			targetFileSystem,
		)
	})

	if err != nil {
		return err
	}

	// Prevent the source database from checkpointing from WAL to Page Log
	// while files are being copied
	err = checkpointer.CheckpointBarrier(func() error {
		return copySourceDatabasePageLogsToTargetDatabase(
			sourceDatabaseUuid,
			sourceBranchUuid,
			targetDatabaseUuid,
			targetBranchUuid,
			sourceFileSystem,
			targetFileSystem,
		)
	})

	if err != nil {
		return err
	}

	err = targetFileSystem.ForceCompact()

	if err != nil {
		slog.Error("Error compacting target database:", "error", err)

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
	entries, err := sourceFileSystem.PageLogger.TieredFS.ReadDir(sourceDirectory)

	if err != nil {
		if os.IsNotExist(err) {
			return ErrorRestoreBackupNotFound
		}

		return err
	}

	for _, entry := range entries {
		sourceFilePath := fmt.Sprintf("%s%s", sourceDirectory, entry.Name())
		sourceFile, err := sourceFileSystem.PageLogger.TieredFS.Open(sourceFilePath)

		if err != nil {
			slog.Error("Error opening source file:", "file", sourceFilePath, "error", err)
			return err
		}

		err = targetFileSystem.PageLogger.TieredFS.MkdirAll(targetDirectory, 0750)

		if err != nil {
			slog.Error("Error creating target directory:", "directory", targetDirectory, "error", err)
			return err
		}

		targetFilePath := fmt.Sprintf("%s%s", targetDirectory, entry.Name())

		targetFile, err := targetFileSystem.PageLogger.TieredFS.Create(targetFilePath)

		if err != nil {
			slog.Error("Error writing file:", "file", entry.Name(), "error", err)
			return err
		}

		_, err = io.Copy(targetFile, sourceFile)

		if err != nil {
			slog.Error("Error copying page:", "file", entry.Name(), "error", err)
			return err
		}

		if err = targetFile.Close(); err != nil {
			slog.Error("Error closing target file", "file", targetFilePath, "error", err)
		}

		if err = sourceFile.Close(); err != nil {
			slog.Error("Error closing source file", "file", sourceFilePath, "error", err)
		}
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
			slog.Error("Error parsing entry name:", "file", entry.Name(), "error", err)
			return err
		}

		if rangeNumber > maxRangeNumber {
			continue
		}

		// Copy the file from the source to the target
		sourceFilePath := fmt.Sprintf("%s%s", sourceDirectory, entry.Name())

		sourceFile, err := sourceFileSystem.FileSystem().Open(sourceFilePath)

		if err != nil {
			slog.Error("Error opening source file:", "file", sourceFilePath, "error", err)
			return err
		}

		targetFilePath := fmt.Sprintf("%s%s", targetDirectory, entry.Name())

		targetFile, err := targetFileSystem.FileSystem().Create(targetFilePath)

		if err != nil {
			slog.Error("Error writing file:", "file", targetFilePath, "error", err)
			return err
		}

		_, err = io.Copy(targetFile, sourceFile)

		if err != nil {
			slog.Error("Error copying page:", "file", targetFilePath, "error", err)
			return err
		}

		if err = targetFile.Close(); err != nil {
			slog.Error("Error closing target file:", "file", targetFilePath, "error", err)
		}

		if err = sourceFile.Close(); err != nil {
			slog.Error("Error closing source file:", "file", sourceFilePath, "error", err)
		}
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
		slog.Error("Backup not found for the specified timestamp")
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
		backupFile, err := sourceFileSystem.FileSystem().OpenFile(backupPartPath, os.O_RDWR, 0600)

		if err != nil {
			slog.Error("Error opening backup part:", "file", backupPartPath, "error", err)
			return err
		}

		_, err = backupFile.Seek(0, io.SeekStart) // Ensure we start reading from the beginning

		if err != nil {
			slog.Error("Error seeking to start of backup file:", "file", backupPartPath, "error", err)
			return err
		}

		gzipReader, err := gzip.NewReader(backupFile)

		if err != nil {
			slog.Error("Error creating gzip reader:", "error", err)
			return err
		}

		tarReader := tar.NewReader(gzipReader)

		for {
			header, err := tarReader.Next()

			if errors.Is(err, io.EOF) {
				break
			}

			if err != nil {
				slog.Error("Error reading tar header:", "error", err)

				return err
			}

			switch header.Typeflag {
			case tar.TypeDir:
				// The database directory should only contain files
			case tar.TypeReg:
				data, err := io.ReadAll(tarReader)

				if err != nil {
					slog.Error("Error reading file data:", "file", header.Name, "error", err)
				}

				err = targetFileSystem.FileSystem().WriteFile(
					file.GetDatabaseFileDir(targetDatabaseUuid, targetBranchUuid)+header.Name,
					data,
					0600,
				)

				if err != nil {
					slog.Error("Error writing file:", "file", header.Name, "error", err)

					return err
				}

				if header.Name == "_METADATA" {
					// It is important to reload the metadata after writing to it
					if err := targetFileSystem.Metadata().Load(); err != nil {
						slog.Error("Error reloading metadata:", "error", err)
						return err
					}
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
	checkpointer Checkpointer,
	onComplete func(func() error) error,
) error {
	// Truncate the timestamp to the start of the hour
	startOfHourTimestamp := time.Unix(0, backupTimestamp).UTC().Truncate(time.Hour).UnixNano()
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
		log.Println("Error reading rollback logs directory:", "directory", directory, "error", err)
		return err
	}

	rollbackLogTimestamps := make([]int64, 0)

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		entryTimeNano, err := strconv.ParseInt(entry.Name(), 10, 64) // Convert string to int64

		if err != nil {
			slog.Error("Error parsing entry name as timestamp:", "file", entry.Name(), "error", err)
			return err
		}

		entryTime := time.Unix(0, entryTimeNano).UTC() // Convert UnixNano to time.Time
		entryTimestamp := entryTime.UnixNano()

		if startOfHourTimestamp < entryTimestamp {
			continue
		}

		rollbackLogTimestamps = append(rollbackLogTimestamps, entryTimestamp)
	}

	if len(rollbackLogTimestamps) == 0 {
		slog.Warn("No rollback logs found for the specified timestamp")
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
		checkpointer,
	)

	if err != nil {
		slog.Error("Error copying source database to target database", "error", err)
		return err
	}

	// Open the rollback logs and restore the pages
	for _, entryTimestamp := range rollbackLogTimestamps {
		rollbackLog, err := rollbackLogger.GetLog(entryTimestamp)

		if err != nil {
			slog.Error("Error opening rollback log:", "error", err)
			return err
		}

		rollbackLogEntries, doneChannel, errorChannel := rollbackLog.ReadForTimestamp(backupTimestamp)

	rollbackLogTimestampsLoop:
		for {
			select {
			case <-doneChannel:
				break rollbackLogTimestampsLoop
			case err := <-errorChannel:
				slog.Error("Error reading rollback log:", "error", err)
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
						slog.Error("Error writing page:", "page", rollbackLogEntry.PageNumber, "error", err)
						return err
					}
				}
			}
		}
	}

	// Truncate the database file
	err = targetFileSystem.Truncate(int64(restorePoint.PageCount) * c.PageSize)

	if err != nil {
		slog.Error("Error truncating database file:", "error", err)
		return err
	}

	err = targetFileSystem.Metadata().SetPageCount(restorePoint.PageCount)

	if err != nil {
		slog.Error("Error setting page count:", "error", err)
		return err
	}

	if onComplete == nil {
		return nil
	}

	// Wrap things up after running this callback
	return onComplete(func() error {
		return nil
	})
}
