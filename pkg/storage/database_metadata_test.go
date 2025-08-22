package storage_test

import (
	"fmt"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/config"
	"github.com/litebase/litebase/pkg/file"
	"github.com/litebase/litebase/pkg/server"
	"github.com/litebase/litebase/pkg/storage"
)

func TestMetaData(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		t.Run("TestNewDatabaseMetadata", func(t *testing.T) {
			mockDatabase := test.MockDatabase(app)

			localDatabaseFileSystem := storage.NewDurableDatabaseFileSystem(
				app.Cluster.LocalFS(),
				app.Cluster.LocalFS(),
				app.DatabaseManager.PageLogManager().Get(mockDatabase.DatabaseID, mockDatabase.DatabaseBranchID, app.Cluster.LocalFS()),
				config.StorageModeLocal,
				mockDatabase.DatabaseID,
				mockDatabase.DatabaseBranchID,
				4096,
			)

			databaseMetadata, err := storage.NewDatabaseMetadata(
				localDatabaseFileSystem,
				mockDatabase.DatabaseID,
				mockDatabase.DatabaseBranchID,
			)

			if err != nil {
				t.Errorf("error creating database metadata: %v", err)
			}

			if databaseMetadata.DatabaseBranchID != mockDatabase.DatabaseBranchID {
				t.Errorf("expected branch uuid %s, got %s", mockDatabase.DatabaseBranchID, databaseMetadata.DatabaseBranchID)
			}

			if databaseMetadata.DatabaseID != mockDatabase.DatabaseID {
				t.Errorf("expected database uuid %s, got %s", mockDatabase.DatabaseID, databaseMetadata.DatabaseID)
			}

			if databaseMetadata.PageSize != 4096 {
				t.Errorf("expected page size 4096, got %d", databaseMetadata.PageSize)
			}

			if databaseMetadata.PageCount != 0 {
				t.Errorf("expected page count 0, got %d", databaseMetadata.PageCount)
			}
		})

		t.Run("Close", func(t *testing.T) {
			mockDatabase := test.MockDatabase(app)

			localDatabaseFileSystem := storage.NewDurableDatabaseFileSystem(
				app.Cluster.LocalFS(),
				app.Cluster.LocalFS(),
				app.DatabaseManager.PageLogManager().Get(mockDatabase.DatabaseID, mockDatabase.DatabaseBranchID, app.Cluster.LocalFS()),
				config.StorageModeLocal,
				mockDatabase.DatabaseID,
				mockDatabase.DatabaseBranchID,
				4096,
			)

			databaseMetadata, _ := storage.NewDatabaseMetadata(
				localDatabaseFileSystem,
				mockDatabase.DatabaseID,
				mockDatabase.DatabaseBranchID,
			)

			err := databaseMetadata.Close()

			if err != nil {
				t.Errorf("error closing database metadata: %v", err)
			}
		})

		t.Run("TestDatabaseMetadataFile", func(t *testing.T) {
			mockDatabase := test.MockDatabase(app)

			localDatabaseFileSystem := storage.NewDurableDatabaseFileSystem(
				app.Cluster.LocalFS(),
				app.Cluster.LocalFS(),
				app.DatabaseManager.PageLogManager().Get(mockDatabase.DatabaseID, mockDatabase.DatabaseBranchID, app.Cluster.LocalFS()),
				config.StorageModeLocal,
				mockDatabase.DatabaseID,
				mockDatabase.DatabaseBranchID,
				4096,
			)

			databaseMetadata, _ := storage.NewDatabaseMetadata(
				localDatabaseFileSystem,
				mockDatabase.DatabaseID,
				mockDatabase.DatabaseBranchID,
			)

			_, err := databaseMetadata.File()

			if err != nil {
				t.Errorf("error getting database metadata file: %v", err)
			}

			if err := databaseMetadata.Close(); err != nil {
				t.Errorf("error closing database metadata: %v", err)
			}

			_, err = databaseMetadata.File()

			if err != nil {
				t.Errorf("expected no error when getting database metadata file after close, got: %v", err)
			}
		})

		t.Run("FileSize", func(t *testing.T) {
			mockDatabase := test.MockDatabase(app)

			localDatabaseFileSystem := storage.NewDurableDatabaseFileSystem(
				app.Cluster.LocalFS(),
				app.Cluster.LocalFS(),
				app.DatabaseManager.PageLogManager().Get(mockDatabase.DatabaseID, mockDatabase.DatabaseBranchID, app.Cluster.LocalFS()),
				config.StorageModeLocal,
				mockDatabase.DatabaseID,
				mockDatabase.DatabaseBranchID,
				4096,
			)

			databaseMetadata, _ := storage.NewDatabaseMetadata(
				localDatabaseFileSystem,
				mockDatabase.DatabaseID,
				mockDatabase.DatabaseBranchID,
			)

			if databaseMetadata.FileSize() != 0 {
				t.Errorf("expected file size 0, got %d", databaseMetadata.FileSize())
			}

			databaseMetadata.PageCount = 10

			if databaseMetadata.FileSize() != 40960 {
				t.Errorf("expected file size 40960, got %d", databaseMetadata.FileSize())
			}
		})

		t.Run("Load", func(t *testing.T) {
			mockDatabase := test.MockDatabase(app)

			localDatabaseFileSystem := storage.NewDurableDatabaseFileSystem(
				app.Cluster.LocalFS(),
				app.Cluster.LocalFS(),
				app.DatabaseManager.PageLogManager().Get(mockDatabase.DatabaseID, mockDatabase.DatabaseBranchID, app.Cluster.LocalFS()),
				config.StorageModeLocal,
				mockDatabase.DatabaseID,
				mockDatabase.DatabaseBranchID,
				4096,
			)

			databaseMetadata, _ := storage.NewDatabaseMetadata(
				localDatabaseFileSystem,
				mockDatabase.DatabaseID,
				mockDatabase.DatabaseBranchID,
			)

			databaseMetadata.PageCount = 10

			err := databaseMetadata.Save()

			if err != nil {
				t.Errorf("error saving database metadata: %v", err)
			}

			err = databaseMetadata.Close()

			if err != nil {
				t.Errorf("error closing database metadata: %v", err)
			}

			databaseMetadata, _ = storage.NewDatabaseMetadata(
				localDatabaseFileSystem,
				mockDatabase.DatabaseID,
				mockDatabase.DatabaseBranchID,
			)

			err = databaseMetadata.Load()

			if err != nil {
				t.Errorf("error loading database metadata: %v", err)
			}

			if databaseMetadata.PageCount != 10 {
				t.Errorf("expected page count 10, got %d", databaseMetadata.PageCount)
			}
		})

		t.Run("Path", func(t *testing.T) {
			mockDatabase := test.MockDatabase(app)

			localDatabaseFileSystem := storage.NewDurableDatabaseFileSystem(
				app.Cluster.LocalFS(),
				app.Cluster.LocalFS(),
				app.DatabaseManager.PageLogManager().Get(mockDatabase.DatabaseID, mockDatabase.DatabaseBranchID, app.Cluster.LocalFS()),
				config.StorageModeLocal,
				mockDatabase.DatabaseID,
				mockDatabase.DatabaseBranchID,
				4096,
			)

			databaseMetadata, _ := storage.NewDatabaseMetadata(
				localDatabaseFileSystem,
				mockDatabase.DatabaseID,
				mockDatabase.DatabaseBranchID,
			)

			path := databaseMetadata.Path()

			if path != fmt.Sprintf("%s_METADATA", file.GetDatabaseFileDir(mockDatabase.DatabaseID, mockDatabase.DatabaseBranchID)) {
				t.Errorf("expected path local/_METADATA, got %s", path)
			}
		})

		t.Run("Save", func(t *testing.T) {
			mockDatabase := test.MockDatabase(app)

			localDatabaseFileSystem := storage.NewDurableDatabaseFileSystem(
				app.Cluster.LocalFS(),
				app.Cluster.LocalFS(),
				app.DatabaseManager.PageLogManager().Get(mockDatabase.DatabaseID, mockDatabase.DatabaseBranchID, app.Cluster.LocalFS()),
				config.StorageModeLocal,
				mockDatabase.DatabaseID,
				mockDatabase.DatabaseBranchID,
				4096,
			)

			databaseMetadata, _ := storage.NewDatabaseMetadata(
				localDatabaseFileSystem,
				mockDatabase.DatabaseID,
				mockDatabase.DatabaseBranchID,
			)

			databaseMetadata.PageCount = 10

			err := databaseMetadata.Save()

			if err != nil {
				t.Errorf("error saving database metadata: %v", err)
			}

			// Close
			err = databaseMetadata.Close()

			if err != nil {
				t.Errorf("error closing database metadata: %v", err)
			}

			// Load
			databaseMetadata, _ = storage.NewDatabaseMetadata(
				localDatabaseFileSystem,
				mockDatabase.DatabaseID,
				mockDatabase.DatabaseBranchID,
			)

			if databaseMetadata.PageCount != 10 {
				t.Errorf("expected page count 10, got %d", databaseMetadata.PageCount)
			}
		})

		t.Run("SetPageCount", func(t *testing.T) {
			mockDatabase := test.MockDatabase(app)

			localDatabaseFileSystem := storage.NewDurableDatabaseFileSystem(
				app.Cluster.LocalFS(),
				app.Cluster.LocalFS(),
				app.DatabaseManager.PageLogManager().Get(mockDatabase.DatabaseID, mockDatabase.DatabaseBranchID, app.Cluster.LocalFS()),
				config.StorageModeLocal,
				mockDatabase.DatabaseID,
				mockDatabase.DatabaseBranchID,
				4096,
			)

			databaseMetadata, _ := storage.NewDatabaseMetadata(
				localDatabaseFileSystem,
				mockDatabase.DatabaseID,
				mockDatabase.DatabaseBranchID,
			)

			if err := databaseMetadata.SetPageCount(100); err != nil {
				t.Errorf("error setting page count: %v", err)
			}

			if databaseMetadata.PageCount != 100 {
				t.Errorf("expected page count 100, got %d", databaseMetadata.PageCount)
			}
		})
	})
}
