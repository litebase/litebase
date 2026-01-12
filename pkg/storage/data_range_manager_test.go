package storage_test

import (
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/server"
	"github.com/litebase/litebase/pkg/storage"
)

func TestDataRangeManager(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		t.Run("TestNewDataRangeManager", func(t *testing.T) {
			drm := storage.NewDataRangeManager(nil)

			if drm == nil {
				t.Error("Expected DataRangeManager to be initialized")
			}
		})

		t.Run("Acquire", func(t *testing.T) {
			drm := storage.NewDataRangeManager(nil)

			drm.Acquire(12345)

			if usage, ok := drm.RangeUsage()[12345]; !ok || usage != 1 {
				t.Errorf("Expected range usage for timestamp 12345 to be 1, got %d", usage)
			}

			drm.Acquire(12345)

			if usage, ok := drm.RangeUsage()[12345]; !ok || usage != 2 {
				t.Errorf("Expected range usage for timestamp 12345 to be 2, got %d", usage)
			}
		})

		t.Run("Close", func(t *testing.T) {
			drm := storage.NewDataRangeManager(nil)

			err := drm.Close()

			if err != nil {
				t.Errorf("Expected Close to succeed, got error: %v", err)
			}
		})

		t.Run("Get", func(t *testing.T) {
			mock := test.MockDatabase(app)

			drm := storage.NewDataRangeManager(
				app.DatabaseManager.Resources(mock.Branch).FileSystem(),
			)

			r, err := drm.Get(1)

			if err != nil {
				t.Errorf("Expected Get to succeed, got error: %v", err)
			}

			if r == nil {
				t.Error("Expected Get to return a Range, got nil")
			}

			r, err = drm.Get(2)

			if err != nil {
				t.Errorf("Expected Get to succeed, got error: %v", err)
			}

			if r == nil {
				t.Error("Expected Get to return a Range, got nil")
			}
		})

		t.Run("GetOldestTimestamp", func(t *testing.T) {
			mock := test.MockDatabase(app)

			drm := storage.NewDataRangeManager(
				app.DatabaseManager.Resources(mock.Branch).FileSystem(),
			)

			drm.Acquire(12345)
			drm.Acquire(67890)

			oldest := drm.GetOldestTimestamp()

			if oldest != 12345 {
				t.Errorf("Expected oldest timestamp to be 12345, got %d", oldest)
			}
		})

		t.Run("RangeUsage", func(t *testing.T) {
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
		})

		t.Run("Release", func(t *testing.T) {
			mock := test.MockDatabase(app)

			drm := storage.NewDataRangeManager(
				app.DatabaseManager.Resources(mock.Branch).FileSystem(),
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

		t.Run("Remove", func(t *testing.T) {
			mock := test.MockDatabase(app)

			drm := storage.NewDataRangeManager(
				app.DatabaseManager.Resources(mock.Branch).FileSystem(),
			)

			_, err := drm.Get(12345)

			if err != nil {
				t.Errorf("Expected Get to succeed, got error: %v", err)
			}

			err = drm.Remove(12345)

			if err != nil {
				t.Errorf("Expected Remove to succeed, got error: %v", err)
			}
		})
	})
}
