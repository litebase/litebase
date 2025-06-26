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

func TestNewDatabaseMetadata(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mockDatabase := test.MockDatabase(app)

		localDatabaseFileSystem := storage.NewDurableDatabaseFileSystem(
			app.Cluster.LocalFS(),
			app.Cluster.LocalFS(),
			app.DatabaseManager.PageLogManager().Get(mockDatabase.DatabaseID, mockDatabase.BranchID, app.Cluster.LocalFS()),
			config.StorageModeLocal,
			mockDatabase.DatabaseID,
			mockDatabase.BranchID,
			4096,
		)

		databaseMetadata, err := storage.NewDatabaseMetadata(
			localDatabaseFileSystem,
			mockDatabase.DatabaseID,
			mockDatabase.BranchID,
		)

		if err != nil {
			t.Errorf("error creating database metadata: %v", err)
		}

		if databaseMetadata.BranchID != mockDatabase.BranchID {
			t.Errorf("expected branch uuid %s, got %s", mockDatabase.BranchID, databaseMetadata.BranchID)
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
}

func TestDatabaseMetadata_Close(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mockDatabase := test.MockDatabase(app)

		localDatabaseFileSystem := storage.NewDurableDatabaseFileSystem(
			app.Cluster.LocalFS(),
			app.Cluster.LocalFS(),
			app.DatabaseManager.PageLogManager().Get(mockDatabase.DatabaseID, mockDatabase.BranchID, app.Cluster.LocalFS()),
			config.StorageModeLocal,
			mockDatabase.DatabaseID,
			mockDatabase.BranchID,
			4096,
		)

		databaseMetadata, _ := storage.NewDatabaseMetadata(
			localDatabaseFileSystem,
			mockDatabase.DatabaseID,
			mockDatabase.BranchID,
		)

		err := databaseMetadata.Close()

		if err != nil {
			t.Errorf("error closing database metadata: %v", err)
		}
	})
}

func TestDatabaseMetadataFile(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mockDatabase := test.MockDatabase(app)

		localDatabaseFileSystem := storage.NewDurableDatabaseFileSystem(
			app.Cluster.LocalFS(),
			app.Cluster.LocalFS(),
			app.DatabaseManager.PageLogManager().Get(mockDatabase.DatabaseID, mockDatabase.BranchID, app.Cluster.LocalFS()),
			config.StorageModeLocal,
			mockDatabase.DatabaseID,
			mockDatabase.BranchID,
			4096,
		)

		databaseMetadata, _ := storage.NewDatabaseMetadata(
			localDatabaseFileSystem,
			mockDatabase.DatabaseID,
			mockDatabase.BranchID,
		)

		_, err := databaseMetadata.File()

		if err != nil {
			t.Errorf("error getting database metadata file: %v", err)
		}

		databaseMetadata.Close()

		_, err = databaseMetadata.File()

		if err != nil {
			t.Errorf("expected no error when getting database metadata file after close, got: %v", err)
		}
	})
}

func TestDatabaseMetadata_FileSize(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mockDatabase := test.MockDatabase(app)

		localDatabaseFileSystem := storage.NewDurableDatabaseFileSystem(
			app.Cluster.LocalFS(),
			app.Cluster.LocalFS(),
			app.DatabaseManager.PageLogManager().Get(mockDatabase.DatabaseID, mockDatabase.BranchID, app.Cluster.LocalFS()),
			config.StorageModeLocal,
			mockDatabase.DatabaseID,
			mockDatabase.BranchID,
			4096,
		)

		databaseMetadata, _ := storage.NewDatabaseMetadata(
			localDatabaseFileSystem,
			mockDatabase.DatabaseID,
			mockDatabase.BranchID,
		)

		if databaseMetadata.FileSize() != 0 {
			t.Errorf("expected file size 0, got %d", databaseMetadata.FileSize())
		}

		databaseMetadata.PageCount = 10

		if databaseMetadata.FileSize() != 40960 {
			t.Errorf("expected file size 40960, got %d", databaseMetadata.FileSize())
		}
	})
}

func TestDatabaseMetadata_Load(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mockDatabase := test.MockDatabase(app)

		localDatabaseFileSystem := storage.NewDurableDatabaseFileSystem(
			app.Cluster.LocalFS(),
			app.Cluster.LocalFS(),
			app.DatabaseManager.PageLogManager().Get(mockDatabase.DatabaseID, mockDatabase.BranchID, app.Cluster.LocalFS()),
			config.StorageModeLocal,
			mockDatabase.DatabaseID,
			mockDatabase.BranchID,
			4096,
		)

		databaseMetadata, _ := storage.NewDatabaseMetadata(
			localDatabaseFileSystem,
			mockDatabase.DatabaseID,
			mockDatabase.BranchID,
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
			mockDatabase.BranchID,
		)

		err = databaseMetadata.Load()

		if err != nil {
			t.Errorf("error loading database metadata: %v", err)
		}

		if databaseMetadata.PageCount != 10 {
			t.Errorf("expected page count 10, got %d", databaseMetadata.PageCount)
		}
	})
}

func TestDatabaseMetadata_Path(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mockDatabase := test.MockDatabase(app)

		localDatabaseFileSystem := storage.NewDurableDatabaseFileSystem(
			app.Cluster.LocalFS(),
			app.Cluster.LocalFS(),
			app.DatabaseManager.PageLogManager().Get(mockDatabase.DatabaseID, mockDatabase.BranchID, app.Cluster.LocalFS()),
			config.StorageModeLocal,
			mockDatabase.DatabaseID,
			mockDatabase.BranchID,
			4096,
		)

		databaseMetadata, _ := storage.NewDatabaseMetadata(
			localDatabaseFileSystem,
			mockDatabase.DatabaseID,
			mockDatabase.BranchID,
		)

		path := databaseMetadata.Path()

		if path != fmt.Sprintf("%s_METADATA", file.GetDatabaseFileDir(mockDatabase.DatabaseID, mockDatabase.BranchID)) {
			t.Errorf("expected path local/_METADATA, got %s", path)
		}
	})
}

func TestDatabaseMetadata_Save(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mockDatabase := test.MockDatabase(app)

		localDatabaseFileSystem := storage.NewDurableDatabaseFileSystem(
			app.Cluster.LocalFS(),
			app.Cluster.LocalFS(),
			app.DatabaseManager.PageLogManager().Get(mockDatabase.DatabaseID, mockDatabase.BranchID, app.Cluster.LocalFS()),
			config.StorageModeLocal,
			mockDatabase.DatabaseID,
			mockDatabase.BranchID,
			4096,
		)

		databaseMetadata, _ := storage.NewDatabaseMetadata(
			localDatabaseFileSystem,
			mockDatabase.DatabaseID,
			mockDatabase.BranchID,
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
			mockDatabase.BranchID,
		)

		if databaseMetadata.PageCount != 10 {
			t.Errorf("expected page count 10, got %d", databaseMetadata.PageCount)
		}
	})
}

func TestDatabaseMetadata_SetPageCount(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mockDatabase := test.MockDatabase(app)

		localDatabaseFileSystem := storage.NewDurableDatabaseFileSystem(
			app.Cluster.LocalFS(),
			app.Cluster.LocalFS(),
			app.DatabaseManager.PageLogManager().Get(mockDatabase.DatabaseID, mockDatabase.BranchID, app.Cluster.LocalFS()),
			config.StorageModeLocal,
			mockDatabase.DatabaseID,
			mockDatabase.BranchID,
			4096,
		)

		databaseMetadata, _ := storage.NewDatabaseMetadata(
			localDatabaseFileSystem,
			mockDatabase.DatabaseID,
			mockDatabase.BranchID,
		)

		databaseMetadata.SetPageCount(100)

		if databaseMetadata.PageCount != 100 {
			t.Errorf("expected page count 100, got %d", databaseMetadata.PageCount)
		}
	})
}
