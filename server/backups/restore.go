package backups

import (
	"litebase/internal/config"
	"litebase/server/file"
	"litebase/server/storage"
	"log"
	"sort"
	"strconv"
	"time"
)

// TODO: Set source and target database UUIDs. A new database should be created beforehand.
// TOOD: Update the database key of the target database to the source database key on success?
func RestoreFromTimestamp(
	databaseUuid string,
	branchUuid string,
	backupTimestamp int64,
	fileSystem *storage.DurableDatabaseFileSystem,
	onComplete func(func() error) error,
) error {
	// Truncate the timestamp to the start of the hour
	startOfHourTimestamp := time.Unix(backupTimestamp, 0).UTC().Truncate(time.Hour).Unix()

	rollbackLogger := NewRollbackLogger(databaseUuid, branchUuid)

	restorePoint, err := GetRestorePoint(databaseUuid, branchUuid, backupTimestamp)

	if err != nil {
		return err
	}

	// Walk the files in the page versions directory
	directory := file.GetDatabaseRollbackDirectory(databaseUuid, branchUuid)

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

	// Sort the timestamps in descending order
	sort.Slice(rollbackLogTimestamps, func(i, j int) bool {
		return rollbackLogTimestamps[i] > rollbackLogTimestamps[j]
	})

	// Open the rollback logs and restore the pages
	for _, entryTimestamp := range rollbackLogTimestamps {
		rollbackLog, err := rollbackLogger.GetLog(entryTimestamp)

		if err != nil {
			log.Println("Error opening rollback log:", err)
			return err
		}

		frames, err := rollbackLog.ReadAfter(backupTimestamp)

		if err != nil {
			log.Println("Error reading rollback log:", err)
			return err
		}

		for _, frame := range frames {
			timestamps := make([]int64, len(frame))

			for i, rollbackLogEntry := range frame {
				timestamps[i] = rollbackLogEntry.Timestamp
			}

			for _, rollbackLogEntry := range frame {
				_, err = fileSystem.WriteWithoutWriteHook(func() (int, error) {
					return fileSystem.WriteAt(rollbackLogEntry.Data, file.PageOffset(rollbackLogEntry.PageNumber, config.Get().PageSize))
				})

				if err != nil {
					log.Println("Error writing page:", rollbackLogEntry.PageNumber, err)
					return err
				}
			}
		}

		rollbackLog.Close()
	}

	// Truncate the database file
	err = fileSystem.Truncate(int64(restorePoint.PageCount) * config.Get().PageSize)

	if err != nil {
		log.Println("Error truncating database file:", err)
		return err
	}

	err = fileSystem.Metadata().SetPageCount(restorePoint.PageCount)

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
