package storage_test

import (
	"fmt"
	"litebase/internal/config"
	"litebase/internal/test"
	"litebase/server/file"
	"litebase/server/storage"
	"testing"
)

func TestNewDatabaseMetadata(t *testing.T) {
	test.Run(t, func() {
		mockDatabase := test.MockDatabase()

		localDatabaseFileSystem := storage.NewDurableDatabaseFileSystem(
			storage.LocalFS(),
			config.STORAGE_MODE_LOCAL,
			mockDatabase.DatabaseUuid,
			mockDatabase.BranchUuid,
			4096,
		)

		databaseMetadata, err := storage.NewDatabaseMetadata(
			localDatabaseFileSystem,
			mockDatabase.DatabaseUuid,
			mockDatabase.BranchUuid,
		)

		if err != nil {
			t.Errorf("error creating database metadata: %v", err)
		}

		if databaseMetadata.BranchUuid != mockDatabase.BranchUuid {
			t.Errorf("expected branch uuid %s, got %s", mockDatabase.BranchUuid, databaseMetadata.BranchUuid)
		}

		if databaseMetadata.DatabaseUuid != mockDatabase.DatabaseUuid {
			t.Errorf("expected database uuid %s, got %s", mockDatabase.DatabaseUuid, databaseMetadata.DatabaseUuid)
		}

		if databaseMetadata.PageSize != 4096 {
			t.Errorf("expected page size 4096, got %d", databaseMetadata.PageSize)
		}

		if databaseMetadata.PageCount != 0 {
			t.Errorf("expected page count 0, got %d", databaseMetadata.PageCount)
		}
	})
}

func TestDatabaseMetadataClose(t *testing.T) {
	test.Run(t, func() {
		mockDatabase := test.MockDatabase()

		localDatabaseFileSystem := storage.NewDurableDatabaseFileSystem(
			storage.LocalFS(),
			config.STORAGE_MODE_LOCAL,
			mockDatabase.DatabaseUuid,
			mockDatabase.BranchUuid,
			4096,
		)

		databaseMetadata, _ := storage.NewDatabaseMetadata(
			localDatabaseFileSystem,
			mockDatabase.DatabaseUuid,
			mockDatabase.BranchUuid,
		)

		err := databaseMetadata.Close()

		if err != nil {
			t.Errorf("error closing database metadata: %v", err)
		}
	})
}

func TestDatabaseMetadataFile(t *testing.T) {
	test.Run(t, func() {
		mockDatabase := test.MockDatabase()

		localDatabaseFileSystem := storage.NewDurableDatabaseFileSystem(
			storage.LocalFS(),
			config.STORAGE_MODE_LOCAL,
			mockDatabase.DatabaseUuid,
			mockDatabase.BranchUuid,
			4096,
		)

		databaseMetadata, _ := storage.NewDatabaseMetadata(
			localDatabaseFileSystem,
			mockDatabase.DatabaseUuid,
			mockDatabase.BranchUuid,
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

func TestDatabaseMetadataFileSize(t *testing.T) {
	test.Run(t, func() {
		mockDatabase := test.MockDatabase()

		localDatabaseFileSystem := storage.NewDurableDatabaseFileSystem(
			storage.LocalFS(),
			config.STORAGE_MODE_LOCAL,
			mockDatabase.DatabaseUuid,
			mockDatabase.BranchUuid,
			4096,
		)

		databaseMetadata, _ := storage.NewDatabaseMetadata(
			localDatabaseFileSystem,
			mockDatabase.DatabaseUuid,
			mockDatabase.BranchUuid,
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

func TestDatabaseMetadataLoad(t *testing.T) {
	test.Run(t, func() {
		mockDatabase := test.MockDatabase()

		localDatabaseFileSystem := storage.NewDurableDatabaseFileSystem(
			storage.LocalFS(),
			config.STORAGE_MODE_LOCAL,
			mockDatabase.DatabaseUuid,
			mockDatabase.BranchUuid,
			4096,
		)

		databaseMetadata, _ := storage.NewDatabaseMetadata(
			localDatabaseFileSystem,
			mockDatabase.DatabaseUuid,
			mockDatabase.BranchUuid,
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
			mockDatabase.DatabaseUuid,
			mockDatabase.BranchUuid,
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

func TestDatabaseMetadataPath(t *testing.T) {
	test.Run(t, func() {
		mockDatabase := test.MockDatabase()

		localDatabaseFileSystem := storage.NewDurableDatabaseFileSystem(
			storage.LocalFS(),
			config.STORAGE_MODE_LOCAL,
			mockDatabase.DatabaseUuid,
			mockDatabase.BranchUuid,
			4096,
		)

		databaseMetadata, _ := storage.NewDatabaseMetadata(
			localDatabaseFileSystem,
			mockDatabase.DatabaseUuid,
			mockDatabase.BranchUuid,
		)

		path := databaseMetadata.Path()

		if path != fmt.Sprintf("%s/_METADATA", file.GetDatabaseFileDir(mockDatabase.DatabaseUuid, mockDatabase.BranchUuid)) {
			t.Errorf("expected path local/_METADATA, got %s", path)
		}
	})
}

func TestDatabaseMetadataSave(t *testing.T) {
	test.Run(t, func() {
		mockDatabase := test.MockDatabase()

		localDatabaseFileSystem := storage.NewDurableDatabaseFileSystem(
			storage.LocalFS(),
			config.STORAGE_MODE_LOCAL,
			mockDatabase.DatabaseUuid,
			mockDatabase.BranchUuid,
			4096,
		)

		databaseMetadata, _ := storage.NewDatabaseMetadata(
			localDatabaseFileSystem,
			mockDatabase.DatabaseUuid,
			mockDatabase.BranchUuid,
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
			mockDatabase.DatabaseUuid,
			mockDatabase.BranchUuid,
		)

		if databaseMetadata.PageCount != 10 {
			t.Errorf("expected page count 10, got %d", databaseMetadata.PageCount)
		}
	})
}

func TestDatabaseMetadataSetPageCount(t *testing.T) {
	test.Run(t, func() {
		mockDatabase := test.MockDatabase()

		localDatabaseFileSystem := storage.NewDurableDatabaseFileSystem(
			storage.LocalFS(),
			config.STORAGE_MODE_LOCAL,
			mockDatabase.DatabaseUuid,
			mockDatabase.BranchUuid,
			4096,
		)

		databaseMetadata, _ := storage.NewDatabaseMetadata(
			localDatabaseFileSystem,
			mockDatabase.DatabaseUuid,
			mockDatabase.BranchUuid,
		)

		databaseMetadata.SetPageCount(100)

		if databaseMetadata.PageCount != 100 {
			t.Errorf("expected page count 100, got %d", databaseMetadata.PageCount)
		}
	})
}
