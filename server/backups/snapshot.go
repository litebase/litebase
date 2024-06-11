package backups

import (
	"encoding/binary"
	"fmt"
	"litebase/server/file"
	"litebase/server/storage"
	"os"
	"time"
)

type Snapshot struct {
	RestorePoints SnapshotRestorePoints `json:"restore_points"`
	Timestamp     uint64                `json:"timestamp"`
}

type SnapshotRestorePoints struct {
	Data  []uint64 `json:"data"`
	Start uint64   `json:"start"`
	End   uint64   `json:"end"`
	Total int      `json:"total"`
}

type RestorePoint struct {
	Timestamp uint64
	PageCount uint32
}

func NewSnapshot(timestamp uint64) *Snapshot {
	snapshot := &Snapshot{
		Timestamp: timestamp,
	}

	return snapshot
}

func GetSnapshotPath(databaseUuid string, branchUuid string) string {
	directory := file.GetDatabaseFileDir(databaseUuid, branchUuid)

	return fmt.Sprintf("%s/logs/snapshots", directory)
}

// Get Snapshots from the snapshot file segmented by day. We will get the first
// checkpoint of the day and use it as the snapshot for that day.
func GetSnapshots(databaseUuid string, branchUuid string) ([]Snapshot, error) {
	snapshotFile, err := storage.FS().OpenFile(GetSnapshotPath(databaseUuid, branchUuid), os.O_RDONLY, 0644)

	if err != nil {
		return nil, err
	}

	defer snapshotFile.Close()

	snapshots := map[time.Time]Snapshot{}

	// Read the snapshots 8 bytes at a time and get one timestamp per day
	// This is because we only need one snapshot per day.
	for {
		data := make([]byte, 64)

		_, err := snapshotFile.Read(data)

		if err != nil {
			break
		}

		timestamp := binary.LittleEndian.Uint64(data[0:8])

		// Get the start of the day of the timestamp
		startOfDay := time.Unix(int64(timestamp), 0).UTC()
		startOfDay = time.Date(startOfDay.Year(), startOfDay.Month(), startOfDay.Day(), 0, 0, 0, 0, time.UTC)

		if _, ok := snapshots[startOfDay]; !ok {
			snapshots[startOfDay] = Snapshot{
				Timestamp: uint64(startOfDay.Unix()),
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

func GetSnapshot(databaseUuid string, branchUuid string, timestamp uint64) (Snapshot, error) {
	snapshotFile, err := storage.FS().OpenFile(GetSnapshotPath(databaseUuid, branchUuid), os.O_RDONLY, 0644)

	if err != nil {
		return Snapshot{}, err
	}

	defer snapshotFile.Close()

	var snapshot Snapshot

	// Get the start of the day of the timestamp
	snapshotStartOfDay := time.Unix(int64(timestamp), 0).UTC()
	snapshotStartOfDay = time.Date(snapshotStartOfDay.Year(), snapshotStartOfDay.Month(), snapshotStartOfDay.Day(), 0, 0, 0, 0, time.UTC)

	var currentSnapshotDay time.Time

	for {
		data := make([]byte, 64)

		_, err := snapshotFile.Read(data)

		if err != nil {
			break
		}

		t := binary.LittleEndian.Uint64(data)

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
				Timestamp: uint64(startOfDay.Unix()),
				RestorePoints: SnapshotRestorePoints{
					Data:  []uint64{t},
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

func GetRestorePoint(databaseUuid string, branchUuid string, timestamp uint64) (RestorePoint, error) {
	snapshotFile, err := storage.FS().OpenFile(GetSnapshotPath(databaseUuid, branchUuid), os.O_RDONLY, 0644)

	if err != nil {
		return RestorePoint{}, err
	}

	defer snapshotFile.Close()

	var restorePoint RestorePoint

	for {
		data := make([]byte, 64)

		_, err := snapshotFile.Read(data)

		if err != nil {
			break
		}

		t := binary.LittleEndian.Uint64(data)

		if t == timestamp {
			restorePoint = RestorePoint{
				Timestamp: t,
				PageCount: binary.LittleEndian.Uint32(data[8:12]),
			}

			break
		}
	}

	return restorePoint, nil
}

func (s Snapshot) IsEmpty() bool {
	return s.Timestamp == 0
}
