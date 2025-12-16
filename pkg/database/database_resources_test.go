package database_test

import (
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/server"
)

func TestDatabaseResources(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		t.Run("ExportManager", func(t *testing.T) {
			mock := test.MockDatabase(app)

			resources := app.DatabaseManager.Resources(mock.DatabaseID, mock.DatabaseBranchID)

			exportManager, err := resources.ExportManager()

			if err != nil {
				t.Fatal(err)
			}

			if exportManager == nil {
				t.Fatal("Expected export manager to be non-nil")
			}

			// Calling it again should return the same instance
			exportManager2, err := resources.ExportManager()

			if err != nil {
				t.Fatal(err)
			}

			if exportManager != exportManager2 {
				t.Fatal("Expected same export manager instance on second call")
			}
		})

		t.Run("ExportManager_WithFileSystem", func(t *testing.T) {
			mock := test.MockDatabase(app)

			resources := app.DatabaseManager.Resources(mock.DatabaseID, mock.DatabaseBranchID)

			// Get export manager
			exportManager, err := resources.ExportManager()

			if err != nil {
				t.Fatal(err)
			}

			// Verify it can create an export (which requires the file system)
			export, err := exportManager.Create()

			if err != nil {
				t.Fatal(err)
			}

			if export == nil {
				t.Fatal("Expected export to be non-nil")
			}

			// Clean up
			exportManager.Clear()
		})
	})
}
