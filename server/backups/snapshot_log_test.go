package backups_test

import (
	"encoding/binary"
	"fmt"
	"litebase/internal/test"
	"litebase/server"
	"litebase/server/backups"
	"testing"
	"time"
)

func TestSnapGetSnapshotPath(t *testing.T) {
	test.Run(t, func(app *server.App) {
		mock := test.MockDatabase(app)
		expectedPath := fmt.Sprintf("_databases/%s/%s/logs/snapshots/123", mock.DatabaseId, mock.BranchId)
		actualPath := backups.GetSnapshotPath(mock.DatabaseId, mock.BranchId, 123)

		if actualPath != expectedPath {
			t.Fatalf("Expected path %s, got %s", expectedPath, actualPath)
		}
	})
}

func TestNewSnapshot(t *testing.T) {
	test.Run(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		snapshot := backups.NewSnapshot(mock.DatabaseId, mock.BranchId, time.Now().Unix(), time.Now().Unix())

		if snapshot.BranchId != mock.BranchId {
			t.Fatalf("Expected branch uuid %s, got %s", mock.BranchId, snapshot.BranchId)
		}

		if snapshot.DatabaseId != mock.DatabaseId {
			t.Fatalf("Expected database uuid %s, got %s", mock.DatabaseId, snapshot.DatabaseId)
		}

		if snapshot.RestorePoints.Total != 0 {
			t.Fatalf("Expected total restore points to be 0, got %d", snapshot.RestorePoints.Total)
		}

		if snapshot.RestorePoints.Start != snapshot.RestorePoints.End {
			t.Fatalf("Expected start and end to be the same for the first snapshot")
		}
	})
}

func TestSnapshotClose(t *testing.T) {
	test.Run(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		snapshotLogger := app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).SnapshotLogger()
		defer snapshotLogger.Close()

		checkpointerLogger := backups.NewSnapshotLogger(mock.DatabaseId, mock.BranchId)
		defer checkpointerLogger.Close()

		// Simulate writing a snapshot to the file
		timestamp := time.Now().Unix()
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
}

func TestSnapshotGetRestorePoints(t *testing.T) {
	test.Run(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		snappshotLogger := app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).SnapshotLogger()
		checkpointerLogger := backups.NewSnapshotLogger(mock.DatabaseId, mock.BranchId)
		defer checkpointerLogger.Close()

		// Simulate writing a snapshot to the file
		timestamp := time.Now().Add(time.Duration(-1) * time.Hour).Unix()
		checkpointerLogger.Log(timestamp, int64(100))

		snapshot, err := snappshotLogger.GetSnapshot(timestamp)

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
}

func TestSnapshotLoad(t *testing.T) {
	test.Run(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		snappshotLogger := app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).SnapshotLogger()
		defer snappshotLogger.Close()

		checkpointerLogger := backups.NewSnapshotLogger(mock.DatabaseId, mock.BranchId)
		defer checkpointerLogger.Close()

		// Simulate writing a snapshot to the file
		timestamp := time.Now().Add(time.Duration(-1) * time.Hour).Unix()
		checkpointerLogger.Log(timestamp, int64(100))

		snapshot, err := snappshotLogger.GetSnapshot(timestamp)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

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
}

func TestSnapshotLog(t *testing.T) {
	test.Run(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		snapshotLogger := backups.NewSnapshotLogger(mock.DatabaseId, mock.BranchId)
		defer snapshotLogger.Close()

		// Simulate writing a snapshot to the file
		timestamp := time.Now().Unix()
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
}
