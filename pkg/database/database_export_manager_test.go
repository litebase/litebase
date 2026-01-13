package database_test

import (
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/database"
	"github.com/litebase/litebase/pkg/server"
)

func TestDatabaseExportManager(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		t.Run("NewDatabaseExportManager", func(t *testing.T) {
			mock := test.MockDatabase(app)

			dfs := app.DatabaseManager.Resources(mock.Branch).FileSystem()
			manager := database.NewDatabaseExportManager(mock.DatabaseID, mock.DatabaseBranchID, dfs)

			if manager == nil {
				t.Fatal("Expected manager to be non-nil")
			}
		})

		t.Run("Create", func(t *testing.T) {
			mock := test.MockDatabase(app)

			dfs := app.DatabaseManager.Resources(mock.Branch).FileSystem()
			manager := database.NewDatabaseExportManager(mock.DatabaseID, mock.DatabaseBranchID, dfs)

			export, err := manager.Create()

			if err != nil {
				t.Fatal(err)
			}

			if export == nil {
				t.Fatal("Expected export to be non-nil")
			}

			if export.ID == "" {
				t.Fatal("Expected export to have an ID")
			}

			if export.StartedAt.IsZero() {
				t.Fatal("Expected export to have a start time")
			}

			// Cleanup - release the compaction barrier
			manager.Clear()
		})

		t.Run("Create_OnlyOneExportAllowed", func(t *testing.T) {
			mock := test.MockDatabase(app)

			dfs := app.DatabaseManager.Resources(mock.Branch).FileSystem()
			manager := database.NewDatabaseExportManager(mock.DatabaseID, mock.DatabaseBranchID, dfs)

			// Create first export
			export1, err := manager.Create()

			if err != nil {
				t.Fatal(err)
			}

			if export1 == nil {
				t.Fatal("Expected first export to be non-nil")
			}

			// Try to create second export
			_, err = manager.Create()

			if err == nil {
				t.Fatal("Expected error when creating second export")
			}

			expectedError := "an export is already active"

			if err.Error() != expectedError {
				t.Fatalf("Expected error '%s', got '%s'", expectedError, err.Error())
			}

			// Cleanup - release the compaction barrier
			manager.Clear()
		})

		t.Run("Get", func(t *testing.T) {
			mock := test.MockDatabase(app)

			dfs := app.DatabaseManager.Resources(mock.Branch).FileSystem()
			manager := database.NewDatabaseExportManager(mock.DatabaseID, mock.DatabaseBranchID, dfs)

			// Create an export
			created, err := manager.Create()

			if err != nil {
				t.Fatal(err)
			}

			// Get the export
			retrieved, err := manager.Get()

			if err != nil {
				t.Fatal(err)
			}

			if retrieved.ID != created.ID {
				t.Fatalf("Expected export ID to be %s, got %s", created.ID, retrieved.ID)
			}

			if !retrieved.StartedAt.Equal(created.StartedAt) {
				t.Fatal("Expected retrieved export to have same start time as created export")
			}

			// Cleanup - release the compaction barrier
			manager.Clear()
		})

		t.Run("Get_NoActiveExport", func(t *testing.T) {
			mock := test.MockDatabase(app)

			dfs := app.DatabaseManager.Resources(mock.Branch).FileSystem()
			manager := database.NewDatabaseExportManager(mock.DatabaseID, mock.DatabaseBranchID, dfs)

			// Try to get export when none exists
			_, err := manager.Get()

			if err == nil {
				t.Fatal("Expected error when getting export with no active export")
			}

			expectedError := "no active export"
			if err.Error() != expectedError {
				t.Fatalf("Expected error '%s', got '%s'", expectedError, err.Error())
			}
		})

		t.Run("Clear", func(t *testing.T) {
			mock := test.MockDatabase(app)

			dfs := app.DatabaseManager.Resources(mock.Branch).FileSystem()
			manager := database.NewDatabaseExportManager(mock.DatabaseID, mock.DatabaseBranchID, dfs)

			// Create an export
			_, err := manager.Create()

			if err != nil {
				t.Fatal(err)
			}

			// Verify export exists
			_, err = manager.Get()

			if err != nil {
				t.Fatal("Expected export to exist after create")
			}

			// Clear the export
			manager.Clear()

			// Verify export no longer exists
			_, err = manager.Get()

			if err == nil {
				t.Fatal("Expected error after clearing export")
			}
		})

		t.Run("CreateAfterClear", func(t *testing.T) {
			mock := test.MockDatabase(app)

			dfs := app.DatabaseManager.Resources(mock.Branch).FileSystem()
			manager := database.NewDatabaseExportManager(mock.DatabaseID, mock.DatabaseBranchID, dfs)

			// Create first export
			export1, err := manager.Create()

			if err != nil {
				t.Fatal(err)
			}

			// Clear the export
			manager.Clear()

			// Create second export (should succeed)
			export2, err := manager.Create()

			if err != nil {
				t.Fatal(err)
			}

			if export2 == nil {
				t.Fatal("Expected second export to be non-nil")
			}

			if export1.ID == export2.ID {
				t.Fatal("Expected second export to have different ID from first export")
			}

			// Cleanup - release the compaction barrier
			manager.Clear()
		})
	})
}
