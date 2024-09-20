package backups

import (
	"encoding/binary"
	"fmt"
	"io"
	"litebase/server/file"
	"litebase/server/storage"
	"os"
	"time"
)

type Snapshot struct {
	RestorePoints SnapshotRestorePoints `json:"restore_points"`
	Timestamp     int64                 `json:"timestamp"`
}

type SnapshotRestorePoints struct {
	Data  []int64 `json:"data"`
	Start int64   `json:"start"`
	End   int64   `json:"end"`
	Total int     `json:"total"`
}

type RestorePoint struct {
	Timestamp int64
	PageCount int64
}

/*
Return the path to the snapshot log file for a database.
*/
func GetSnapshotPath(databaseUuid string, branchUuid string) string {
	return fmt.Sprintf(
		"%s/logs/snapshots/SNAPSHOT_LOG",
		file.GetDatabaseFileBaseDir(databaseUuid, branchUuid),
	)
}

/*
Get Snapshots from the snapshot file segmented by day. We will get the first
checkpoint of the day and use it as the snapshot for that day.
*/
func GetSnapshots(databaseUuid string, branchUuid string) ([]Snapshot, error) {
openFile:
	snapshotFile, err := storage.TieredFS().OpenFile(GetSnapshotPath(databaseUuid, branchUuid), SNAPSHOT_LOG_FLAGS, 0644)

	if err != nil {
		if os.IsNotExist(err) {
			err := storage.TieredFS().MkdirAll(fmt.Sprintf("%s/logs/snapshots", file.GetDatabaseFileBaseDir(databaseUuid, branchUuid)), 0755)

			if err != nil {
				return nil, err
			}

			goto openFile
		} else {
			return nil, err
		}
	}

	defer snapshotFile.Close()

	snapshots := map[time.Time]Snapshot{}

	_, err = snapshotFile.Seek(0, io.SeekStart)

	if err != nil {
		return nil, err
	}

	// Read the snapshots 64 bytes at a time and get one timestamp per day
	// This is because we only need one snapshot per day.
	for {
		data := make([]byte, 64)

		_, err := snapshotFile.Read(data)

		if err != nil {
			break
		}

		timestamp := int64(binary.LittleEndian.Uint64(data[0:8]))

		// Get the start of the day of the timestamp
		startOfDay := time.Unix(timestamp, 0).UTC()
		startOfDay = time.Date(startOfDay.Year(), startOfDay.Month(), startOfDay.Day(), 0, 0, 0, 0, time.UTC)

		if _, ok := snapshots[startOfDay]; !ok {
			snapshots[startOfDay] = Snapshot{
				Timestamp: startOfDay.Unix(),
				RestorePoints: SnapshotRestorePoints{
					Start: timestamp,
					End:   timestamp,
					Total: 1,
				},
			}
		} else {
			snapshots[startOfDay] = Snapshot{
				Timestamp: snapshots[startOfDay].Timestamp,
				RestorePoints: SnapshotRestorePoints{
					Start: snapshots[startOfDay].RestorePoints.Start,
					End:   timestamp,
					Total: snapshots[startOfDay].RestorePoints.Total + 1,
				},
			}
		}
	}

	values := make([]Snapshot, 0, len(snapshots))

	for _, snapshot := range snapshots {
		values = append(values, snapshot)
	}

	return values, nil
}

/*
Get a single snapshot for a specific timestamp. This method does not include
All the restore points for the day, just the first one.
*/
func GetSnapshot(databaseUuid string, branchUuid string, timestamp int64) (Snapshot, error) {
	snapshotFile, err := storage.TieredFS().OpenFile(GetSnapshotPath(databaseUuid, branchUuid), SNAPSHOT_LOG_FLAGS, 0644)

	if err != nil {
		return Snapshot{}, err
	}

	defer snapshotFile.Close()

	_, err = snapshotFile.Seek(0, io.SeekStart)

	if err != nil {
		return Snapshot{}, err
	}

	var snapshot Snapshot

	// Get the start of the day of the timestamp
	snapshotStartOfDay := time.Unix(timestamp, 0).UTC()
	snapshotStartOfDay = time.Date(snapshotStartOfDay.Year(), snapshotStartOfDay.Month(), snapshotStartOfDay.Day(), 0, 0, 0, 0, time.UTC)

	var currentSnapshotDay time.Time

	for {
		data := make([]byte, 64)

		_, err := snapshotFile.Read(data)

		if err != nil {
			break
		}

		t := int64(binary.LittleEndian.Uint64(data))

		// Get the start of the day of the timestamp
		startOfDay := time.Unix(int64(t), 0).UTC()
		startOfDay = time.Date(startOfDay.Year(), startOfDay.Month(), startOfDay.Day(), 0, 0, 0, 0, time.UTC)

		// We can stop once we have passed the snapshot day
		if snapshotStartOfDay.Before(startOfDay) {
			break
		}

		// If we have not reached the snapshot day, continue
		if snapshotStartOfDay != startOfDay {
			continue
		}
		if currentSnapshotDay.IsZero() {
			currentSnapshotDay = startOfDay

			snapshot = Snapshot{
				Timestamp: startOfDay.Unix(),
				RestorePoints: SnapshotRestorePoints{
					Data:  []int64{t},
					Start: t,
					End:   t,
					Total: 1,
				},
			}
		} else {
			snapshot.RestorePoints.Data = append(snapshot.RestorePoints.Data, t)
			snapshot.RestorePoints.End = t
			snapshot.RestorePoints.Total++
		}
	}

	return snapshot, nil
}

/*
Get a specific restore point from the snapshot file.
*/
func GetRestorePoint(databaseUuid string, branchUuid string, timestamp int64) (RestorePoint, error) {
	snapshotFile, err := storage.TieredFS().OpenFile(GetSnapshotPath(databaseUuid, branchUuid), SNAPSHOT_LOG_FLAGS, 0644)

	if err != nil {
		if os.IsNotExist(err) {
			return RestorePoint{}, nil
		}

		return RestorePoint{}, err
	}

	defer snapshotFile.Close()

	_, err = snapshotFile.Seek(0, io.SeekStart)

	if err != nil {
		return RestorePoint{}, err
	}

	var restorePoint RestorePoint

	for {
		data := make([]byte, 64)

		_, err := snapshotFile.Read(data)

		if err != nil {
			break
		}

		t := int64(binary.LittleEndian.Uint64(data))

		if int64(t) == timestamp {
			restorePoint = RestorePoint{
				Timestamp: t,
				PageCount: int64(binary.LittleEndian.Uint32(data[8:12])),
			}

			break
		}
	}

	return restorePoint, nil
}

/*
Determine if the snapshot is empty.
*/
func (s Snapshot) IsEmpty() bool {
	return s.Timestamp == 0
}
