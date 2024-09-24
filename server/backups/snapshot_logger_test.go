package backups_test

import (
	"encoding/binary"
	"litebase/internal/test"
	"litebase/server/backups"
	"litebase/server/database"
	"testing"
	"time"
)

func TestNewSnapshotLogger(t *testing.T) {
	test.Run(t, func() {
		mock := test.MockDatabase()
		logger := backups.NewSnapshotLogger(mock.DatabaseUuid, mock.BranchUuid)

		if logger == nil {
			t.Fatal("Expected logger to be created, got nil")
		}

		if logger.DatabaseUuid != mock.DatabaseUuid {
			t.Fatalf("Expected databaseUuid %s, got %s", mock.DatabaseUuid, logger.DatabaseUuid)
		}

		if logger.BranchUuid != mock.BranchUuid {
			t.Fatalf("Expected branchUuid %s, got %s", mock.BranchUuid, logger.BranchUuid)
		}
	})
}

func TestSnapshotLoggerClose(t *testing.T) {
	test.Run(t, func() {
		mock := test.MockDatabase()
		logger := backups.NewSnapshotLogger(mock.DatabaseUuid, mock.BranchUuid)

		if err := logger.Close(); err != nil {
			t.Fatalf("Expected no error on close, got %v", err)
		}
	})
}

func TestSnapshotLoggerGetSnapshot(t *testing.T) {
	test.Run(t, func() {
		mock := test.MockDatabase()

		snapshotLogger := database.Resources(mock.DatabaseUuid, mock.BranchUuid).SnapshotLogger()
		checkpointerLogger := backups.NewSnapshotLogger(mock.DatabaseUuid, mock.BranchUuid)
		defer checkpointerLogger.Close()

		// Simulate writing a snapshot to the file
		timestamp := time.Now().Unix()
		err := checkpointerLogger.Log(timestamp, int64(1))

		if err != nil {
			t.Fatalf("Failed to log snapshot: %v", err)
		}

		snapshot, err := snapshotLogger.GetSnapshot(timestamp)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if snapshot.Timestamp == 0 {
			t.Fatalf("Expected a valid timestamp, got 0")
		}

		if snapshot.RestorePoints.Total != 0 {
			t.Fatalf("Expected 0 restore points, got %d", snapshot.RestorePoints.Total)
		}
	})
}

func TestSnapshotLoggerGetSnapshots(t *testing.T) {
	test.Run(t, func() {
		mock := test.MockDatabase()
		snapshotLogger := database.Resources(mock.DatabaseUuid, mock.BranchUuid).SnapshotLogger()
		keys := snapshotLogger.Keys()

		if len(keys) != 0 {
			t.Fatalf("Expected 0 snapshots, got %d", len(keys))
		}

		// Simulate writing a snapshot to the file
		for i := 0; i < 5; i++ {
			timestamp := time.Now().Add(-time.Duration(5-i) * time.Second).Unix()
			snapshotLogger.Log(timestamp, int64(i))
		}

		snapshots, err := snapshotLogger.GetSnapshots()

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		keys = snapshotLogger.Keys()

		snapshot := snapshots[keys[0]]

		snapshot.Load()

		if snapshot.RestorePoints.Total != 5 {
			t.Fatalf("Expected 5 snapshots, got %d", snapshot.RestorePoints.Total)
		}
	})
}

func TestSnapshotLoggerGetSnapshotsWithRestorePoints(t *testing.T) {
	test.Run(t, func() {
		mock := test.MockDatabase()
		snapshotLogger := database.Resources(mock.DatabaseUuid, mock.BranchUuid).SnapshotLogger()

		// Simulate writing a snapshot to the file
		snapshotLogger.Log(time.Now().Add(-3*time.Second).Unix(), int64(1))
		snapshotLogger.Log(time.Now().Add(-2*time.Second).Unix(), int64(2))
		snapshotLogger.Log(time.Now().Add(-1*time.Second).Unix(), int64(3))

		snapshots, err := snapshotLogger.GetSnapshotsWithRestorePoints()

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		keys := snapshotLogger.Keys()

		snapshot := snapshots[keys[0]]

		if snapshot.RestorePoints.Total != 3 {
			t.Fatalf("Expected 3 restore points, got %d", snapshot.RestorePoints.Total)
		}

		if snapshot.RestorePoints.Start == snapshot.RestorePoints.End {
			t.Fatalf("Expected start and end to be different for the first snapshot")
		}

		if len(snapshot.RestorePoints.Data) != 3 {
			t.Fatalf("Expected 3 restore points to be loaded, got %d", len(snapshot.RestorePoints.Data))
		}

		if snapshot.RestorePoints.Total != 3 {
			t.Fatalf("Expected 3 restore points to be totaled, got %d", snapshot.RestorePoints.Total)
		}
	})
}

func TestSnapshotLoggerLog(t *testing.T) {
	test.Run(t, func() {
		mock := test.MockDatabase()
		logger := backups.NewSnapshotLogger(mock.DatabaseUuid, mock.BranchUuid)
		timestamps := make([]int64, 0)

		for i := 0; i < 10; i++ {
			// Timestamps sub seconds to avoid collisions
			timestamp := time.Now().Add(time.Duration(10-i) * time.Second).UnixNano()
			timestamps = append(timestamps, timestamp)
			err := logger.Log(timestamp, int64(i))

			if err != nil {
				t.Fatalf("Expected no error on File(), got %v", err)
			}
		}

		// read the file to verify the logs were written
		snapshot, err := logger.GetSnapshot(timestamps[0])

		if snapshot == nil {
			t.Fatalf("Expected snapshot to be created, got nil")
		}

		file := snapshot.File

		if err != nil {
			t.Fatalf("Expected no error on File(), got %v", err)
		}

		entry := make([]byte, 64)

		for i, timestamp := range timestamps {
			_, err := file.Read(entry)

			if err != nil {
				break
			}

			if len(entry) == 0 {
				break
			}

			entryTimestamp := int64(binary.LittleEndian.Uint64(entry[0:8]))

			if entryTimestamp != timestamp {
				t.Fatal("Expected valid log entry, got nil")
			}

			pageCount := binary.LittleEndian.Uint32(entry[8:12])

			if pageCount != uint32(i) {
				t.Fatal("Expected valid log entry, got nil")
			}
		}
	})
}
