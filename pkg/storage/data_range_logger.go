package storage

import (
	"bufio"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"

	internalStorage "github.com/litebase/litebase/internal/storage"
	"github.com/litebase/litebase/pkg/file"
)

// The data range logger is responsible for logging the names of ranges that are
// no longer the current version. When the data range manager need to run garbage
// collection, it can use this log to determine which ranges are safe to delete.
type DataRangeLogger struct {
	drm   *DataRangeManager
	file  internalStorage.File
	mutex *sync.Mutex
}

type DataRangeLogEntry struct {
	ID          string
	RangeNumber int64
	Timestamp   int64
}

// Create a new instance of the data range logger.
func NewDataRangeLogger(drm *DataRangeManager) *DataRangeLogger {
	drl := &DataRangeLogger{
		drm:   drm,
		mutex: &sync.Mutex{},
	}

	return drl
}

// All returns all log entries from the data range log file.
func (drl *DataRangeLogger) All() ([]DataRangeLogEntry, error) {
	drl.mutex.Lock()
	defer drl.mutex.Unlock()

	f, err := drl.File()

	if err != nil {
		return nil, err
	}

	// Seek to beginning of file
	_, err = f.Seek(0, io.SeekStart)

	if err != nil {
		return nil, err
	}

	var entries []DataRangeLogEntry

	scanner := bufio.NewScanner(f)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		parts := strings.Split(line, "_")

		if len(parts) != 2 {
			continue
		}

		rangeNumber, err := strconv.ParseInt(parts[0], 10, 64)

		if err != nil {
			slog.Error("Failed to parse range number from data range log entry", "entry", line, "error", err)
			continue
		}

		timestamp, err := strconv.ParseInt(parts[1], 10, 64)

		if err != nil {
			slog.Error("Failed to parse timestamp from data range log entry", "entry", line, "error", err)
			continue
		}

		entries = append(entries, DataRangeLogEntry{
			ID:          line,
			RangeNumber: rangeNumber,
			Timestamp:   timestamp,
		})
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return entries, nil
}

// Append adds a new log entry to the data range log file.
func (drl *DataRangeLogger) Append(rangeID string) error {
	drl.mutex.Lock()
	defer drl.mutex.Unlock()

	f, err := drl.File()

	if err != nil {
		return err
	}

	// Seek to end of file
	_, err = f.Seek(0, io.SeekEnd)

	if err != nil {
		return err
	}

	// Write the range ID with a newline
	_, err = f.Write([]byte(rangeID + "\n"))

	return err
}

// Return the file associated with the data range log, opening it if necessary.
func (drl *DataRangeLogger) File() (internalStorage.File, error) {
	var err error

	if drl.file == nil {
	tryOpen:
		drl.file, err = drl.drm.dfs.FileSystem().OpenFile(drl.Path(), os.O_CREATE|os.O_RDWR, 0600)

		if err != nil {
			if os.IsNotExist(err) {
				err := drl.drm.dfs.FileSystem().MkdirAll(file.GetDatabaseFileDir(drl.drm.dfs.databaseId, drl.drm.dfs.branchId), 0750)

				if err != nil {
					return nil, err
				}

				goto tryOpen
			} else {
				return nil, err
			}
		}
	}

	return drl.file, nil
}

// Return the path for the data range log file.
func (drl *DataRangeLogger) Path() string {
	return fmt.Sprintf("%s_RANGE_LOG", file.GetDatabaseFileDir(drl.drm.dfs.databaseId, drl.drm.dfs.branchId))
}

// Refresh updates the log by rewriting it with only the provided entries,
// effectively removing logs that have been garbage collected.
func (drl *DataRangeLogger) Refresh(validEntries []DataRangeLogEntry) error {
	drl.mutex.Lock()
	defer drl.mutex.Unlock()

	// Close existing file if open
	if drl.file != nil {
		drl.file.Close()
		drl.file = nil
	}

	// Remove the existing log file
	err := drl.drm.dfs.FileSystem().Remove(drl.Path())

	if err != nil && !os.IsNotExist(err) {
		return err
	}

	// Recreate the file with valid entries
	f, err := drl.File()

	if err != nil {
		return err
	}

	// Write all valid entries
	for _, entry := range validEntries {
		_, err = f.Write([]byte(entry.ID + "\n"))

		if err != nil {
			return err
		}
	}

	return nil
}
