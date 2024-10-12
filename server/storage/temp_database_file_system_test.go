package storage_test

import (
	"fmt"
	"litebase/internal/config"
	"litebase/internal/test"
	"litebase/server"
	"litebase/server/storage"
	"testing"
)

func TestNewTempDatabaseFileSystem(t *testing.T) {
	test.Run(t, func(app *server.App) {
		tmpPath := fmt.Sprintf("%s/%s", config.Get().TmpPath, "test")

		fs := storage.NewTempDatabaseFileSystem(tmpPath, "test", "test")

		if fs == nil {
			t.Error("NewTempDatabaseFileSystem() returned nil")
		}

		// Check if the directory exists
		if _, err := storage.TmpFS().Stat(tmpPath); err != nil {
			t.Errorf("Stat() returned an error: %v", err)
		}

		// Check the path
		if fs.Path() != tmpPath {
			t.Errorf("Path() returned unexpected value: %v", fs.Path())
		}
	})
}
