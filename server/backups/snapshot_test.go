package backups_test

import (
	"fmt"
	"litebase/internal/test"
	"litebase/server/backups"
	"testing"
	"time"
)

func TestSnapGetSnapshotPath(t *testing.T) {
	test.Run(t, func() {
		mock := test.MockDatabase()
		expectedPath := fmt.Sprintf("_databases/%s/%s/logs/snapshots/SNAPSHOT_LOG", mock.DatabaseUuid, mock.BranchUuid)
		actualPath := backups.GetSnapshotPath(mock.DatabaseUuid, mock.BranchUuid)

		if actualPath != expectedPath {
			t.Fatalf("Expected path %s, got %s", expectedPath, actualPath)
		}
	})
}

func TestGetSnapshots(t *testing.T) {
	test.Run(t, func() {
		mock := test.MockDatabase()
		snapshots, err := backups.GetSnapshots(mock.DatabaseUuid, mock.BranchUuid)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if len(snapshots) != 0 {
			t.Fatalf("Expected 0 snapshots, got %d", len(snapshots))
		}

		checkpointerLogger := backups.NewCheckpointLogger(mock.DatabaseUuid, mock.BranchUuid)
		defer checkpointerLogger.Close()

		// Simulate writing a snapshot to the file
		for i := 0; i < 5; i++ {
			timestamp := uint64(time.Now().Add(time.Duration(5-i) * time.Second).UnixNano())
			checkpointerLogger.Log(timestamp, uint32(i))
		}

		snapshots, err = backups.GetSnapshots(mock.DatabaseUuid, mock.BranchUuid)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if len(snapshots) != 5 {
			t.Fatalf("Expected 5 snapshots, got %d", len(snapshots))
		}
	})
}

func TestGetSnapshot(t *testing.T) {
	test.Run(t, func() {
		mock := test.MockDatabase()

		checkpointerLogger := backups.NewCheckpointLogger(mock.DatabaseUuid, mock.BranchUuid)
		defer checkpointerLogger.Close()

		// Simulate writing a snapshot to the file
		timestamp := uint64(time.Now().Unix())
		err := checkpointerLogger.Log(timestamp, uint32(1))

		if err != nil {
			t.Fatalf("Failed to log snapshot: %v", err)
		}

		snapshot, err := backups.GetSnapshot(mock.DatabaseUuid, mock.BranchUuid, timestamp)

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

func TestGetRestorePoints(t *testing.T) {
	test.Run(t, func() {
		mock := test.MockDatabase()

		checkpointerLogger := backups.NewCheckpointLogger(mock.DatabaseUuid, mock.BranchUuid)
		defer checkpointerLogger.Close()

		// Simulate writing a snapshot to the file
		timestamp := uint64(time.Now().Add(time.Duration(-1) * time.Hour).Unix())
		checkpointerLogger.Log(timestamp, uint32(100))

		snapshot, err := backups.GetSnapshot(mock.DatabaseUuid, mock.BranchUuid, timestamp)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if snapshot.RestorePoints.Total != 1 {
			t.Fatalf("Expected 1 restore point, got %d", snapshot.RestorePoints.Total)
		}

		if snapshot.RestorePoints.End != snapshot.RestorePoints.Start {
			t.Fatalf("Expected end and start to be the same for the last snapshot")
		}

		restorePoint, err := backups.GetRestorePoint(mock.DatabaseUuid, mock.BranchUuid, snapshot.RestorePoints.Start)

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
