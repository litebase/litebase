package storage_test

import (
	"fmt"
	"testing"

	"github.com/litebase/litebase/internal/test"

	"github.com/litebase/litebase/server/storage"

	"github.com/litebase/litebase/server"
)

func TestNewTempDatabaseFileSystem(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		tmpPath := fmt.Sprintf("%s/%s", app.Config.TmpPath, "test")

		fs := storage.NewTempDatabaseFileSystem(
			app.Cluster.TmpFS(),
			tmpPath,
			"test",
			"test",
		)

		if fs == nil {
			t.Error("NewTempDatabaseFileSystem() returned nil")
		}

		// Check if the directory exists
		if _, err := app.Cluster.TmpFS().Stat(tmpPath); err != nil {
			t.Errorf("Stat() returned an error: %v", err)
		}

		// Check the path
		if fs.Path() != tmpPath {
			t.Errorf("Path() returned unexpected value: %v", fs.Path())
		}
	})
}
