package storage_test

import (
	"bytes"
	"crypto/rand"
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/server"
	"github.com/litebase/litebase/pkg/storage"
)

func TestNewDataRangeManager(t *testing.T) {
	drm := storage.NewDataRangeManager(nil)

	if drm == nil {
		t.Error("Expected DataRangeManager to be initialized")
	}
}

func TestDataRangeManager_Acquire(t *testing.T) {
	drm := storage.NewDataRangeManager(nil)

	drm.Acquire(12345)

	if usage, ok := drm.RangeUsage()[12345]; !ok || usage != 1 {
		t.Errorf("Expected range usage for timestamp 12345 to be 1, got %d", usage)
	}

	drm.Acquire(12345)

	if usage, ok := drm.RangeUsage()[12345]; !ok || usage != 2 {
		t.Errorf("Expected range usage for timestamp 12345 to be 2, got %d", usage)
	}
}

func TestDataRangeManager_Close(t *testing.T) {
	drm := storage.NewDataRangeManager(nil)

	err := drm.Close()

	if err != nil {
		t.Errorf("Expected Close to succeed, got error: %v", err)
	}
}

func TestDataRangeManager_CopyRange(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		drm := storage.NewDataRangeManager(
			app.DatabaseManager.Resources(mock.DatabaseID, mock.DatabaseBranchID).FileSystem(),
		)

		r1Data := make([]byte, 4096)

		rand.Read(r1Data)

		r1, err := drm.Get(1, time.Now().UTC().UnixNano())

		if err != nil {
			t.Errorf("Expected Get to succeed, got error: %v", err)
		}

		_, err = r1.WriteAt(1, r1Data)

		if err != nil {
			t.Errorf("Expected WriteAt to succeed, got error: %v", err)
		}

		_, err = drm.CopyRange(1, time.Now().UTC().UnixNano(), nil)

		if err != nil {
			t.Errorf("Expected CopyRange to succeed, got error: %v", err)
		}

		r2, err := drm.Get(1, time.Now().UTC().UnixNano())

		if err != nil {
			t.Errorf("Expected Get to succeed, got error: %v", err)
		}

		if r1.Timestamp == r2.Timestamp {
			t.Fatalf("Expected timestamps to differ, got %d and %d", r1.Timestamp, r2.Timestamp)
		}

		r2Data := make([]byte, 4096)

		_, err = r2.ReadAt(1, r2Data)

		if err != nil {
			t.Errorf("Expected ReadAt to succeed, got error: %v", err)
		}

		if !bytes.Equal(r1Data, r2Data) {
			t.Errorf("Expected read data to match written data, got %s", r2Data)
		}

		rand.Read(r2Data)

		_, err = r2.WriteAt(1, r2Data)

		if err != nil {
			t.Errorf("Expected WriteAt to succeed, got error: %v", err)
		}

		r12, err := drm.Get(1, r1.Timestamp)

		if err != nil {
			t.Errorf("Expected Get to succeed, got error: %v", err)
		}

		if r1.Timestamp != r12.Timestamp {
			t.Errorf("Expected timestamps to match, got %d and %d", r1.Timestamp, r12.Timestamp)
		}

		r1Data2 := make([]byte, 4096)

		_, err = r12.ReadAt(1, r1Data2)

		if err != nil {
			t.Errorf("Expected ReadAt to succeed, got error: %v", err)
		}

		if bytes.Equal(r1Data2, r2Data) {
			t.Errorf("Did not expect read data to match overwritten data, got %s", r1Data2)
		}
	})
}

func TestDataRangeManager_Get(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		drm := storage.NewDataRangeManager(
			app.DatabaseManager.Resources(mock.DatabaseID, mock.DatabaseBranchID).FileSystem(),
		)

		r, err := drm.Get(1, time.Now().UTC().UnixNano())

		if err != nil {
			t.Errorf("Expected Get to succeed, got error: %v", err)
		}

		if r == nil {
			t.Error("Expected Get to return a Range, got nil")
		}

		r, err = drm.Get(2, time.Now().UTC().UnixNano())

		if err != nil {
			t.Errorf("Expected Get to succeed, got error: %v", err)
		}

		if r == nil {
			t.Error("Expected Get to return a Range, got nil")
		}
	})
}

func TestDataRangeManager_GetOldestTimestamp(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		drm := storage.NewDataRangeManager(
			app.DatabaseManager.Resources(mock.DatabaseID, mock.DatabaseBranchID).FileSystem(),
		)

		drm.Acquire(12345)
		drm.Acquire(67890)

		oldest := drm.GetOldestTimestamp()

		if oldest != 12345 {
			t.Errorf("Expected oldest timestamp to be 12345, got %d", oldest)
		}
	})
}

func TestDataRangeManager_RangeUsage(t *testing.T) {
	drm := storage.NewDataRangeManager(nil)

	drm.Acquire(12345)
	drm.Acquire(67890)

	usage := drm.RangeUsage()

	if len(usage) != 2 {
		t.Errorf("Expected range usage map to have 2 entries, got %d", len(usage))
	}

	if usage[12345] != 1 {
		t.Errorf("Expected range usage for timestamp 12345 to be 1, got %d", usage[12345])
	}

	if usage[67890] != 1 {
		t.Errorf("Expected range usage for timestamp 67890 to be 1, got %d", usage[67890])
	}
}

func TestDataRangeManager_Release(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		drm := storage.NewDataRangeManager(
			app.DatabaseManager.Resources(mock.DatabaseID, mock.DatabaseBranchID).FileSystem(),
		)
		drm.Acquire(12345)

		if drm.RangeUsage()[12345] != 1 {
			t.Errorf("Expected range usage for timestamp 12345 to be 1, got %d", drm.RangeUsage()[12345])
		}

		drm.Release(12345)

		if drm.RangeUsage()[12345] != 0 {
			t.Errorf("Expected range usage for timestamp 12345 to be 0, got %d", drm.RangeUsage()[12345])
		}
	})
}

func TestDataRangeManager_Remove(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		drm := storage.NewDataRangeManager(
			app.DatabaseManager.Resources(mock.DatabaseID, mock.DatabaseBranchID).FileSystem(),
		)

		r, err := drm.Get(12345, time.Now().UTC().UnixNano())

		if err != nil {
			t.Errorf("Expected Get to succeed, got error: %v", err)
		}

		err = drm.Remove(12345, r.Timestamp)

		if err != nil {
			t.Errorf("Expected Remove to succeed, got error: %v", err)
		}
	})
}

func TestDataRangeManager_RunGarbageCollection(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		drm := storage.NewDataRangeManager(
			app.DatabaseManager.Resources(mock.DatabaseID, mock.DatabaseBranchID).FileSystem(),
		)

		// Create ranges with different timestamps
		timestamp1 := time.Now().UTC().UnixNano()
		timestamp2 := timestamp1 + 1000000 // 1ms later
		timestamp3 := timestamp2 + 1000000 // 2ms later

		// Create ranges - these should be eligible for GC
		_, err := drm.Get(1, timestamp1)

		if err != nil {
			t.Fatalf("Expected Get to succeed, got error: %v", err)
		}

		_, err = drm.Get(2, timestamp1)

		if err != nil {
			t.Fatalf("Expected Get to succeed, got error: %v", err)
		}

		// Create a newer range that should NOT be eligible for GC
		r3, err := drm.Get(1, timestamp3)

		if err != nil {
			t.Fatalf("Expected Get to succeed, got error: %v", err)
		}

		// Acquire timestamp3 to make it the oldest active timestamp
		// This means ranges with timestamp1 and timestamp2 should be garbage collected
		drm.Acquire(timestamp3)

		// Verify ranges exist before GC
		rangeUsage := drm.RangeUsage()

		if rangeUsage[timestamp3] != 1 {
			t.Errorf("Expected timestamp3 to have usage 1, got %d", rangeUsage[timestamp3])
		}

		_, err = drm.CopyRange(1, time.Now().UTC().UnixNano(), nil) // Create another range with a new timestamp

		if err != nil {
			t.Fatalf("Expected CopyRange to succeed, got error: %v", err)
		}

		// Run garbage collection
		err = drm.RunGarbageCollection()

		if err != nil {
			t.Errorf("Expected RunGarbageCollection to succeed, got error: %v", err)
		}

		// Verify that old ranges were cleaned up
		// The older ranges (timestamp1) should be removed, but timestamp3 should remain
		rangeUsageAfter := drm.RangeUsage()

		if rangeUsageAfter[timestamp3] != 1 {
			t.Errorf("Expected timestamp3 to still have usage 1 after GC, got %d", rangeUsageAfter[timestamp3])
		}

		// Older timestamps should be cleaned up from rangeUsage
		if _, exists := rangeUsageAfter[timestamp1]; exists {
			t.Errorf("Expected timestamp1 to be cleaned up from rangeUsage, but it still exists")
		}

		// Close the remaining range manually to verify no double-close error
		err = r3.Close()

		if err != nil {
			t.Errorf("Expected Close to succeed, got error: %v", err)
		}

		// Test edge case: GC with no active timestamps
		drm.Release(timestamp3) // Release the last timestamp

		err = drm.RunGarbageCollection()

		if err != nil {
			t.Errorf("Expected RunGarbageCollection with no active timestamps to succeed, got error: %v", err)
		}

		// Ensure the resources are properly cleaned up
		err = drm.Close()

		if err != nil {
			t.Errorf("Expected Close to succeed, got error: %v", err)
		}
	})
}
