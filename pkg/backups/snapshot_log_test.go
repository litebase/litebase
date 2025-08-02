package backups_test

import (
	"encoding/binary"
	"fmt"
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/backups"
	"github.com/litebase/litebase/pkg/server"
)

func TestSnapshotLog(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		t.Run("GetSnapshotPath", func(t *testing.T) {
			mock := test.MockDatabase(app)
			expectedPath := fmt.Sprintf("_databases/%s/%s/logs/snapshots/123", mock.DatabaseID, mock.DatabaseBranchID)
			actualPath := backups.GetSnapshotPath(mock.DatabaseID, mock.DatabaseBranchID, 123)

			if actualPath != expectedPath {
				t.Fatalf("Expected path %s, got %s", expectedPath, actualPath)
			}
		})

		t.Run("NewSnapshot", func(t *testing.T) {
			mock := test.MockDatabase(app)

			snapshot := backups.NewSnapshot(
				app.Cluster.TieredFS(),
				mock.DatabaseID,
				mock.DatabaseBranchID,
				time.Now().UTC().UnixNano(),
				time.Now().UTC().UnixNano(),
			)

			if snapshot.DatabaseBranchID != mock.DatabaseBranchID {
				t.Fatalf("Expected branch uuid %s, got %s", mock.DatabaseBranchID, snapshot.DatabaseBranchID)
			}

			if snapshot.DatabaseID != mock.DatabaseID {
				t.Fatalf("Expected database uuid %s, got %s", mock.DatabaseID, snapshot.DatabaseID)
			}

			if snapshot.RestorePoints.Total != 0 {
				t.Fatalf("Expected total restore points to be 0, got %d", snapshot.RestorePoints.Total)
			}

			if snapshot.RestorePoints.Start != snapshot.RestorePoints.End {
				t.Fatalf("Expected start and end to be the same for the first snapshot")
			}
		})

		t.Run("Close", func(t *testing.T) {
			mock := test.MockDatabase(app)

			snapshotLogger := app.DatabaseManager.Resources(mock.DatabaseID, mock.DatabaseBranchID).SnapshotLogger()
			defer snapshotLogger.Close()

			checkpointerLogger := backups.NewSnapshotLogger(
				app.Cluster.TieredFS(),
				mock.DatabaseID,
				mock.DatabaseBranchID,
			)
			defer checkpointerLogger.Close()

			// Simulate writing a snapshot to the file
			timestamp := time.Now().UTC().UnixNano()
			checkpointerLogger.Log(timestamp, int64(1))

			snapshot, err := snapshotLogger.GetSnapshot(timestamp)

			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}

			err = snapshot.Close()

			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}
		})

		t.Run("GetRestorePoints", func(t *testing.T) {
			mock := test.MockDatabase(app)

			snapshotLogger := app.DatabaseManager.Resources(mock.DatabaseID, mock.DatabaseBranchID).SnapshotLogger()
			checkpointerLogger := backups.NewSnapshotLogger(
				app.Cluster.TieredFS(),
				mock.DatabaseID,
				mock.DatabaseBranchID,
			)
			defer checkpointerLogger.Close()

			// Simulate writing a snapshot to the file
			timestamp := time.Now().UTC().Add(time.Duration(-1) * time.Hour).UnixNano()
			checkpointerLogger.Log(timestamp, 100)

			snapshot, err := snapshotLogger.GetSnapshot(timestamp)

			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}

			if snapshot.RestorePoints.Total != 1 {
				t.Fatalf("Expected 1 restore point, got %d", snapshot.RestorePoints.Total)
			}

			if snapshot.RestorePoints.End != snapshot.RestorePoints.Start {
				t.Fatalf("Expected end and start to be the same for the last snapshot")
			}

			restorePoint, err := snapshot.GetRestorePoint(snapshot.RestorePoints.Start)

			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}

			if restorePoint.Timestamp == 0 {
				t.Fatalf("Expected a valid timestamp, got 0")
			}

			if restorePoint.PageCount != 100 {
				t.Fatalf("Expected page count of 100, got %d", restorePoint.PageCount)
			}
		})

		t.Run("Load", func(t *testing.T) {
			mock := test.MockDatabase(app)

			snapshotLogger := app.DatabaseManager.Resources(mock.DatabaseID, mock.DatabaseBranchID).SnapshotLogger()
			defer snapshotLogger.Close()

			checkpointerLogger := backups.NewSnapshotLogger(
				app.Cluster.TieredFS(),
				mock.DatabaseID,
				mock.DatabaseBranchID,
			)

			defer checkpointerLogger.Close()

			// Create a base time in UTC to ensure both timestamps are on the same day
			baseTime := time.Now().UTC()
			// Set to a specific time during the day to avoid timezone issues
			baseTime = time.Date(baseTime.Year(), baseTime.Month(), baseTime.Day(), 10, 0, 0, 0, time.UTC)

			// Simulate writing a snapshot to the file (2 hours earlier in the same day)
			timestamp := baseTime.Add(-2 * time.Hour).UnixNano()
			checkpointerLogger.Log(timestamp, int64(100))

			snapshot, err := snapshotLogger.GetSnapshot(timestamp)

			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}

			// Simulate writing another snapshot to the file (1 hour later in the same day)
			timestamp = baseTime.Add(-1 * time.Hour).UnixNano()
			checkpointerLogger.Log(timestamp, int64(101))

			if snapshot.RestorePoints.Total != 1 {
				t.Fatalf("Expected 1 restore point, got %d", snapshot.RestorePoints.Total)
			}

			if snapshot.RestorePoints.End != snapshot.RestorePoints.Start {
				t.Fatalf("Expected end and start to be the same for the last snapshot")
			}

			if len(snapshot.RestorePoints.Data) > 1 {
				t.Fatalf("Expected no restore points to be loaded, but got %d", len(snapshot.RestorePoints.Data))
			}

			err = snapshot.Load()

			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}

			if len(snapshot.RestorePoints.Data) != 2 {
				t.Fatalf("Expected 2 restore points to be loaded, got %d", len(snapshot.RestorePoints.Data))
			}
		})

		t.Run("Log", func(t *testing.T) {
			mock := test.MockDatabase(app)

			snapshotLogger := backups.NewSnapshotLogger(
				app.Cluster.TieredFS(),
				mock.DatabaseID,
				mock.DatabaseBranchID,
			)
			defer snapshotLogger.Close()

			// Simulate writing a snapshot to the file
			timestamp := time.Now().UTC().UnixNano()
			err := snapshotLogger.Log(timestamp, int64(1))

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

			snapshot.File.Seek(0, 0)

			entry := make([]byte, 64)

			_, err = snapshot.File.Read(entry)

			if err != nil && err.Error() != "EOF" {
				t.Fatalf("Expected no error on Read(), got %v", err)
			}

			entryTimestamp := int64(binary.LittleEndian.Uint64(entry[0:8]))

			if entryTimestamp != timestamp {
				t.Fatal("Expected valid log entry, got nil")
			}

			pageCount := binary.LittleEndian.Uint32(entry[8:12])

			if pageCount != uint32(1) {
				t.Fatal("Expected valid log entry, got nil")
			}
		})
	})
}
