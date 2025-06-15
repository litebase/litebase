package backups_test

import (
	"encoding/binary"
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/backups"
	"github.com/litebase/litebase/server"
)

func TestNewSnapshotLogger(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)
		logger := backups.NewSnapshotLogger(
			app.Cluster.TieredFS(),
			mock.DatabaseId,
			mock.BranchId,
		)

		if logger == nil {
			t.Fatal("Expected logger to be created, got nil")
		}

		if logger.DatabaseId != mock.DatabaseId {
			t.Fatalf("Expected databaseId %s, got %s", mock.DatabaseId, logger.DatabaseId)
		}

		if logger.BranchId != mock.BranchId {
			t.Fatalf("Expected branchId %s, got %s", mock.BranchId, logger.BranchId)
		}
	})
}

func TestSnapshotLoggerClose(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)
		logger := backups.NewSnapshotLogger(
			app.Cluster.TieredFS(),
			mock.DatabaseId,
			mock.BranchId,
		)

		if err := logger.Close(); err != nil {
			t.Fatalf("Expected no error on close, got %v", err)
		}
	})
}

func TestSnapshotLoggerGetSnapshot(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		snapshotLogger := app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).SnapshotLogger()
		checkpointerLogger := backups.NewSnapshotLogger(
			app.Cluster.TieredFS(),
			mock.DatabaseId,
			mock.BranchId,
		)
		defer checkpointerLogger.Close()

		// Simulate writing a snapshot to the file
		timestamp := time.Now().UTC().UnixNano()
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

		if snapshot.RestorePoints.Total != 1 {
			t.Fatalf("Expected 1 restore point, got %d", snapshot.RestorePoints.Total)
		}
	})
}

func TestSnapshotLoggerGetSnapshots(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)
		snapshotLogger := app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).SnapshotLogger()
		keys := snapshotLogger.Keys()

		if len(keys) != 0 {
			t.Fatalf("Expected 0 snapshots, got %d", len(keys))
		}

		// Simulate writing a snapshot to the file
		for i := range 5 {
			timestamp := time.Now().UTC().Add(-time.Duration(5-i) * time.Second).UnixNano()
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
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)
		snapshotLogger := app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).SnapshotLogger()

		// Simulate writing a snapshot to the file
		snapshotLogger.Log(time.Now().UTC().Add(-3*time.Second).UnixNano(), int64(1))
		snapshotLogger.Log(time.Now().UTC().Add(-2*time.Second).UnixNano(), int64(2))
		snapshotLogger.Log(time.Now().UTC().Add(-1*time.Second).UnixNano(), int64(3))

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
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)
		logger := backups.NewSnapshotLogger(
			app.Cluster.TieredFS(),
			mock.DatabaseId,
			mock.BranchId,
		)
		timestamps := make([]int64, 0)

		for i := range 10 {
			// Timestamps sub seconds to avoid collisions
			timestamp := time.Now().UTC().Add(time.Duration(10-i) * time.Second).UnixNano()
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

func TestSnapshotLogger_Log_Precision(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		logger := backups.NewSnapshotLogger(
			app.Cluster.TieredFS(),
			mock.DatabaseId,
			mock.BranchId,
		)
		timestamps := make([]int64, 0)

		for i := range 10 {
			// Timestamps sub seconds to avoid collisions
			timestamp := time.Now().UTC().UnixNano()
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
