package backups

import (
	"errors"
	"io"
	"log"
	"sort"
	"time"

	"github.com/litebase/litebase/common/config"
	internalStorage "github.com/litebase/litebase/internal/storage"
	"github.com/litebase/litebase/server/file"
	"github.com/litebase/litebase/server/storage"
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
	restoreStartOfHour := time.Unix(b.restorePoint.Timestamp, 0).Truncate(time.Hour).Unix()
	currentStartOfHour := time.Now().Truncate(time.Hour).Unix()
	hourDifference := (currentStartOfHour - restoreStartOfHour) / 3600

	if hourDifference > 0 {
		for i := 1; i <= int(hourDifference); i++ {
			timestamps = append(timestamps, restoreStartOfHour+int64(i)*3600)
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

	// Read the current state of the file
	fileContents, err := io.ReadAll(b.file)

	if err != nil {
		return nil, err
	}

	if len(fileContents) == 0 {
		log.Println("Backup range file is empty")
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
				log.Println("Error reading rollback log entries:", err)
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

	if b.restorePoint.PageCount > endPageNumber {
		return fileContents, nil
	}

	rangePageCount := b.restorePoint.PageCount % storage.RangeMaxPages
	rangeSize := rangePageCount * c.PageSize

	// Truncate the file content to the length of the data
	fileContents = fileContents[:rangeSize]

	return fileContents, err
}
