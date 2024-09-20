package backups

import (
	"fmt"
	"io"
	"litebase/internal/config"
	"litebase/server/file"
	"litebase/server/storage"
	"log"
	"sort"
	"strconv"
	"time"
)

func CopySouceDatabaseToTargetDatabase(
	maxPageNumber int64,
	sourceDatabaseUuid,
	sourceBranchUuid,
	targetDatabaseUuid,
	targetBranchUuid string,
	sourceFileSystem *storage.DurableDatabaseFileSystem,
	targetFileSystem *storage.DurableDatabaseFileSystem,
) error {
	maxRangeNumber := file.PageRange(maxPageNumber, config.Get().PageSize)
	sourceDirectory := file.GetDatabaseFileDir(sourceDatabaseUuid, sourceBranchUuid)
	targetDirectory := file.GetDatabaseFileDir(targetDatabaseUuid, targetBranchUuid)

	// Loop through the files in the source database and copy them to the target database
	entries, err := sourceFileSystem.FileSystem().ReadDir(sourceDirectory)

	if err != nil {
		log.Println("Error reading source directory:", err)
		return err
	}

	for _, entry := range entries {
		if entry.IsDir {
			continue
		}

		if entry.Name[0] == '_' {
			continue
		}

		rangeNumber, err := strconv.ParseInt(entry.Name, 10, 64)

		if err != nil {
			log.Println("Error parsing entry name:", entry.Name, err)
			return err
		}

		if rangeNumber > maxRangeNumber {
			continue
		}

		// Copy the file from the source to the target
		sourceFilePath := fmt.Sprintf("%s/%s", sourceDirectory, entry.Name)
		sourceFile, err := sourceFileSystem.FileSystem().Open(sourceFilePath)

		if err != nil {
			log.Println("Error opening source file:", sourceFilePath, err)
			return err
		}

		targetFilePath := fmt.Sprintf("%s/%s", targetDirectory, entry.Name)

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
	}

	return nil
}

func RestoreFromTimestamp(
	sourceDatabaseUuid string,
	sourceBranchUuid string,
	targetDatabaseUuid string,
	targetBranchUuid string,
	backupTimestamp int64,
	sourceFileSystem *storage.DurableDatabaseFileSystem,
	targetFileSystem *storage.DurableDatabaseFileSystem,
	onComplete func(func() error) error,
) error {
	// Truncate the timestamp to the start of the hour
	startOfHourTimestamp := time.Unix(backupTimestamp, 0).UTC().Truncate(time.Hour).Unix()

	rollbackLogger := NewRollbackLogger(sourceDatabaseUuid, sourceBranchUuid)

	restorePoint, err := GetRestorePoint(sourceDatabaseUuid, sourceBranchUuid, backupTimestamp)

	if err != nil {
		return err
	}

	// Walk the files in the page versions directory
	directory := file.GetDatabaseRollbackDirectory(sourceDatabaseUuid, sourceBranchUuid)

	entries, err := storage.TieredFS().ReadDir(directory)

	if err != nil {
		return err
	}

	rollbackLogTimestamps := make([]int64, 0)

	for _, entry := range entries {
		if entry.IsDir {
			continue
		}

		entryTimestamp, err := strconv.ParseInt(entry.Name, 10, 64)

		if err != nil {
			log.Println("Error parsing entry name:", entry.Name, err)
			return err
		}

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
	err = CopySouceDatabaseToTargetDatabase(
		restorePoint.PageCount,
		sourceDatabaseUuid,
		sourceBranchUuid,
		targetDatabaseUuid,
		targetBranchUuid,
		sourceFileSystem,
		targetFileSystem,
	)

	if err != nil {
		log.Println("Error copying source database to target database:", err)
		return err
	}

	// Open the rollback logs and restore the pages
	for _, entryTimestamp := range rollbackLogTimestamps {
		rollbackLog, err := rollbackLogger.GetLog(entryTimestamp)

		if err != nil {
			log.Println("Error opening rollback log:", err)
			return err
		}

		framesChannel, doneChannel, errorChannel := rollbackLog.ReadForTimestamp(backupTimestamp)

	rollbackLogTimestampsLoop:
		for {
			select {
			case <-doneChannel:
				break rollbackLogTimestampsLoop
			case err := <-errorChannel:
				log.Println("Error reading rollback log:", err)
				return err
			case frame := <-framesChannel:

				timestamps := make([]int64, len(frame))

				for i, rollbackLogEntry := range frame {
					timestamps[i] = rollbackLogEntry.Timestamp
				}

				for _, rollbackLogEntry := range frame {
					_, err = targetFileSystem.WriteWithoutWriteHook(func() (int, error) {
						return targetFileSystem.WriteAt(rollbackLogEntry.Data, file.PageOffset(rollbackLogEntry.PageNumber, config.Get().PageSize))
					})

					if err != nil {
						log.Println("Error writing page:", rollbackLogEntry.PageNumber, err)
						return err
					}
				}
			}
		}
	}

	// Truncate the database file
	err = targetFileSystem.Truncate(int64(restorePoint.PageCount) * config.Get().PageSize)

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
		// Re-key the target database to the source database key

		return nil
	})
}
