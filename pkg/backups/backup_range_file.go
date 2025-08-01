package backups

import (
	"errors"
	"io"
	"log"
	"sort"
	"time"

	internalStorage "github.com/litebase/litebase/internal/storage"
	"github.com/litebase/litebase/pkg/config"
	"github.com/litebase/litebase/pkg/file"
	"github.com/litebase/litebase/pkg/storage"
)

var ErrorBackupRangeFileEmpty = errors.New("backup range file is empty")

type BackupRangeFile struct {
	file           internalStorage.File
	rangeNumber    int64
	restorePoint   RestorePoint
	rollbackLogger *RollbackLogger
}

func ReadBackupRangeFile(
	c *config.Config,
	f internalStorage.File,
	rangeNumber int64,
	restorePoint RestorePoint,
	rollbackLogger *RollbackLogger,
) ([]byte, error) {
	b := &BackupRangeFile{
		file:           f,
		rangeNumber:    rangeNumber,
		restorePoint:   restorePoint,
		rollbackLogger: rollbackLogger,
	}

	var rollbackLogs []*RollbackLog
	var timestamps []int64

	timestamps = append(timestamps, b.restorePoint.Timestamp)

	// Check if now is the current hour of the restore point timestamp.
	// If not, we need to get all the rollback logs for each hour between
	// the restore point timestamp and now.
	restoreStartOfHour := time.Unix(0, b.restorePoint.Timestamp).UTC().Truncate(time.Hour).UnixNano()
	currentStartOfHour := time.Now().UTC().Truncate(time.Hour).UnixNano()

	hourDifference := (currentStartOfHour - restoreStartOfHour) / time.Hour.Nanoseconds()

	if hourDifference > 0 {
		for i := 1; i <= int(hourDifference); i++ {
			timestamps = append(timestamps, restoreStartOfHour+int64(i)*time.Hour.Nanoseconds())
		}
	}

	// Order timestamps in descending order
	sort.Slice(timestamps, func(i, j int) bool {
		return timestamps[i] > timestamps[j]
	})

	// We may need to use multiple logs depending on how long the backup takes
	// And if we cross over into hours that fall outside of the current log
	// and where we are in the backup process.
	for _, timestamp := range timestamps {
		rollbackLog, err := b.rollbackLogger.GetLog(timestamp)

		if err != nil {
			log.Println("Error getting rollback log", err)
			return nil, err
		}

		rollbackLogs = append(rollbackLogs, rollbackLog)
	}

	startPageNumber, endPageNumber := file.PageRangeStartAndEndPageNumbers(
		b.rangeNumber,
		storage.RangeMaxPages,
		c.PageSize,
	)

	// Reset file pointer to beginning
	_, err := b.file.Seek(0, io.SeekStart)

	if err != nil {
		return nil, err
	}

	// Read the current state of the file
	fileContents, err := io.ReadAll(b.file)

	if err != nil {
		return nil, err
	}

	if len(fileContents) == 0 {
		return nil, ErrorBackupRangeFileEmpty
	}

	pageMap := make(map[int64]struct{})

	// Work through the rollback logs to apply any changes made to this range
	for _, rollbackLog := range rollbackLogs {
		if rollbackLog == nil {
			continue
		}

		// Apply the rollback log to the file content
		rollbackLogEntries, doneChannel, errorChannel := rollbackLog.ReadForTimestamp(
			b.restorePoint.Timestamp,
		)

	applyRollBackLogs:
		for {
			select {
			case <-doneChannel:
				break applyRollBackLogs
			case err := <-errorChannel:
				return nil, err
			case frame := <-rollbackLogEntries:
				for _, rollbackLogEntry := range frame {
					if rollbackLogEntry.PageNumber > b.restorePoint.PageCount {
						continue
					}

					// Apply the rollback log entry to the file content if it falls within the range
					if rollbackLogEntry.PageNumber < startPageNumber || rollbackLogEntry.PageNumber > endPageNumber {
						continue
					}

					offset := file.PageRangeOffset(rollbackLogEntry.PageNumber, storage.RangeMaxPages, c.PageSize)

					if offset >= int64(len(fileContents)) {
						log.Println("Offset is greater than the length of the file contents")
						return nil, errors.New("offset is greater than the length of the file contents")
					}

					copy(fileContents[offset:], rollbackLogEntry.Data)
					pageMap[rollbackLogEntry.PageNumber] = struct{}{}
				}
			}
		}
	}

	// Calculate the correct size based on the restore point page count
	// This ensures we don't return more data than the database actually contains
	correctSize := b.restorePoint.PageCount * int64(c.PageSize)

	if correctSize > int64(len(fileContents)) {
		return fileContents, err
	}

	// Truncate to the correct size to ensure we have a valid SQLite file
	return fileContents[:correctSize], err
}
