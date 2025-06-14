package backups

import (
	"log"
	"strconv"
	"sync"
	"time"

	"slices"

	internalStorage "github.com/litebase/litebase/internal/storage"
	"github.com/litebase/litebase/server/file"
	"github.com/litebase/litebase/server/storage"
)

// The SnapshotLogger is used to manage snapshots of the database. The logs
// are stored on disk and organized by day. Each log entry contains a timestamp
// and the number of pages that were written to the snapshot.
type SnapshotLogger struct {
	BranchId          string
	DatabaseId        string
	file              internalStorage.File
	keys              []int64
	logs              map[int64]*Snapshot
	logsLastCleanedAt time.Time
	mutex             *sync.Mutex
	tieredFS          *storage.FileSystem
}

// TODO: When is memory cleanup needed automatically?

// The SnapshotLogger is responsible for logging Snapshots to a file when the
// database is Snapshotted. Each log entry contains a timestamp and the number
// of pages that were written to the snapshot.
func NewSnapshotLogger(tieredFS *storage.FileSystem, databaseId, branchId string) *SnapshotLogger {
	return &SnapshotLogger{
		BranchId:   branchId,
		DatabaseId: databaseId,
		logs:       make(map[int64]*Snapshot),
		mutex:      &sync.Mutex{},
		tieredFS:   tieredFS,
	}
}

// Remove snapshot logs that have not been accessed in the last 5 minutes from
// memory. This is to prevent memory leaks and to keep the memory usage low.
func (sl *SnapshotLogger) cleanupOpenSnapshotLogs() {
	if !sl.logsLastCleanedAt.IsZero() || time.Since(sl.logsLastCleanedAt) <= 5*time.Minute {
		return
	}

	for _, snapshot := range sl.logs {
		if time.Since(time.Unix(0, snapshot.LastAccessedAt).UTC()) > 5*time.Minute {
			if err := snapshot.Close(); err != nil {
				log.Println("Error closing snapshot log", err)
			}

			delete(sl.logs, snapshot.Timestamp)

			for i, key := range sl.keys {
				if key == snapshot.Timestamp {
					sl.keys = slices.Delete(sl.keys, i, i+1)
					break
				}
			}
		}
	}

	sl.logsLastCleanedAt = time.Now().UTC()
}

// Close the logger and the underlying file.
func (sl *SnapshotLogger) Close() error {
	sl.mutex.Lock()
	defer sl.mutex.Unlock()

	if sl.file != nil {
		return sl.file.Close()
	}

	for _, l := range sl.logs {
		if err := l.Close(); err != nil {
			log.Println("Error closing snapshot log", err)
		}
	}

	return nil
}

// Get a single snapshot for a specific timestamp. This method does not include
// all the restore points for the day, just the first one.
func (sl *SnapshotLogger) GetSnapshot(timestamp int64) (*Snapshot, error) {
	sl.mutex.Lock()
	defer sl.mutex.Unlock()

	// Check if this timestamp is already a start-of-day timestamp (i.e., it's in our logs)
	if snapshot, exists := sl.logs[timestamp]; exists {
		if len(snapshot.RestorePoints.Data) <= 0 {
			snapshot.Load()
		}

		sl.cleanupOpenSnapshotLogs()

		return snapshot, nil
	}

	// If not, calculate the start-of-day timestamp
	snapshotStartOfDay := time.Unix(0, timestamp).UTC()

	snapshotStartOfDay = time.Date(snapshotStartOfDay.Year(), snapshotStartOfDay.Month(), snapshotStartOfDay.Day(), 0, 0, 0, 0, time.UTC)
	startOfDayTimestamp := snapshotStartOfDay.UnixNano()

	if _, ok := sl.logs[startOfDayTimestamp]; !ok {
		sl.logs[startOfDayTimestamp] = NewSnapshot(
			sl.tieredFS,
			sl.DatabaseId,
			sl.BranchId,
			startOfDayTimestamp,
			timestamp,
		)

		sl.keys = append(sl.keys, startOfDayTimestamp)
	}

	if len(sl.logs[startOfDayTimestamp].RestorePoints.Data) <= 0 {
		sl.logs[startOfDayTimestamp].Load()
	}

	sl.cleanupOpenSnapshotLogs()

	return sl.logs[startOfDayTimestamp], nil
}

// List Snapshots from the snapshots directory. Each file is a log segmented by day.
// We will get the first checkpoint of the day and use it as the snapshot for that day.
func (sl *SnapshotLogger) GetSnapshots() (map[int64]*Snapshot, error) {
	sl.mutex.Lock()
	defer sl.mutex.Unlock()

	entries, err := sl.tieredFS.ReadDir(
		file.GetDatabaseSnapshotDirectory(sl.DatabaseId, sl.BranchId),
	)

	if err != nil {
		return nil, err
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		timestamp, err := strconv.ParseInt(entry.Name(), 10, 64)

		if err != nil {
			return nil, err
		}

		// if _, ok := sl.logs[timestamp]; ok {
		// 	continue
		// }

		sl.logs[timestamp] = NewSnapshot(
			sl.tieredFS,
			sl.DatabaseId,
			sl.BranchId,
			timestamp,
			0,
		)

		sl.keys = append(sl.keys, timestamp)
	}

	return sl.logs, nil
}

// Load the restore points for all snapshots.
func (sl *SnapshotLogger) GetSnapshotsWithRestorePoints() (map[int64]*Snapshot, error) {
	snapshots, err := sl.GetSnapshots()

	if err != nil {
		return nil, err
	}

	sl.mutex.Lock()
	defer sl.mutex.Unlock()

	for _, snapshot := range snapshots {
		if len(snapshot.RestorePoints.Data) <= 0 {
			snapshot.Load()
		}
	}

	return sl.logs, nil

}

// Write a snapshot log entry to the snapshot log file.
func (sl *SnapshotLogger) Log(timestamp, pageCount int64) error {
	// Get the start of the day of the timestamp
	startOfDayTimestamp := time.Unix(0, timestamp).UTC().Truncate(24 * time.Hour).UnixNano()

	sl.mutex.Lock()
	defer sl.mutex.Unlock()

	if _, ok := sl.logs[startOfDayTimestamp]; !ok {
		sl.logs[startOfDayTimestamp] = NewSnapshot(
			sl.tieredFS,
			sl.DatabaseId,
			sl.BranchId,
			startOfDayTimestamp,
			0,
		)

		sl.keys = append(sl.keys, startOfDayTimestamp)
	}

	return sl.logs[startOfDayTimestamp].Log(timestamp, pageCount)
}

func (sl *SnapshotLogger) Keys() []int64 {
	sl.mutex.Lock()
	defer sl.mutex.Unlock()

	return sl.keys
}
