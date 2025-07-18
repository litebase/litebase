package storage_test

import (
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/server"
	"github.com/litebase/litebase/pkg/storage"
)

func TestNewDataRangeLogger(t *testing.T) {
	drm := storage.NewDataRangeManager(nil)

	if drm == nil {
		t.Error("Expected DataRangeManager to be initialized")
	}

	// Create a new data range logger
	logger := storage.NewDataRangeLogger(drm)

	if logger == nil {
		t.Error("Expected DataRangeLogger to be initialized")
	}
}

func TestDataRangeLogger_All(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		dfs := app.DatabaseManager.Resources("testdb", "main").FileSystem()
		drm := storage.NewDataRangeManager(dfs)
		logger := storage.NewDataRangeLogger(drm)

		entries, err := logger.All()

		if err != nil {
			t.Fatalf("Expected All() to succeed, got error: %v", err)
		}

		err = logger.Append("000000001_1234567890")

		if err != nil {
			t.Fatalf("Expected Append() to succeed, got error: %v", err)
		}

		entries, err = logger.All()

		if err != nil {
			t.Fatalf("Expected All() to succeed, got error: %v", err)
		}

		if entries == nil {
			t.Error("Expected All() to return a slice, got nil")
		}
	})
}

func TestDataRangeLogger_Append(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		dfs := app.DatabaseManager.Resources("testdb", "main").FileSystem()
		drm := storage.NewDataRangeManager(dfs)
		logger := storage.NewDataRangeLogger(drm)

		err := logger.Append("000000001_1234567890")

		if err != nil {
			t.Fatalf("Expected Append() to succeed, got error: %v", err)
		}
	})
}

func TestDataRangeLogger_File(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		dfs := app.DatabaseManager.Resources("testdb", "main").FileSystem()
		drm := storage.NewDataRangeManager(dfs)
		logger := storage.NewDataRangeLogger(drm)

		file, err := logger.File()

		if err != nil {
			t.Errorf("Expected File() to succeed, got error: %v", err)
		}

		if file == nil {
			t.Error("Expected File() to return a valid file")
		}
	})
}

func TestDataRangeLogger_Path(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		dfs := app.DatabaseManager.Resources("testdb", "main").FileSystem()
		drm := storage.NewDataRangeManager(dfs)
		logger := storage.NewDataRangeLogger(drm)

		path := logger.Path()

		if path == "" {
			t.Error("Expected Path() to return a valid path")
		}
	})
}

func TestDataRangeManager_Refresh(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		dfs := app.DatabaseManager.Resources("testdb", "main").FileSystem()
		drm := storage.NewDataRangeManager(dfs)
		logger := storage.NewDataRangeLogger(drm)

		err := logger.Refresh([]storage.DataRangeLogEntry{
			{ID: "000000001_1234567890", RangeNumber: 1, Timestamp: 1234567890},
		})

		if err != nil {
			t.Fatalf("Expected Refresh() to succeed, got error: %v", err)
		}

		entries, err := logger.All()

		if err != nil {
			t.Fatalf("Expected All() to succeed, got error: %v", err)
		}

		if len(entries) != 1 {
			t.Errorf("Expected 1 entry after refresh, got %d", len(entries))
		}

		// Check that the entry is valid
		if entries[0].ID != "000000001_1234567890" {
			t.Errorf("Expected entry ID to be '000000001_1234567890', got '%s'", entries[0].ID)
		}
	})
}
