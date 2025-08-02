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

func TestNewDataRangeIndex(t *testing.T) {
	dri := storage.NewDataRangeIndex(nil)

	if dri == nil {
		t.Error("Expected DataRangeIndex to be initialized")
	}
}

func TestDataRangeIndex_All(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mockDatabase := test.MockDatabase(app)

		dfs := storage.NewDurableDatabaseFileSystem(
			app.Cluster.TieredFS(),
			app.Cluster.NetworkFS(),
			app.DatabaseManager.PageLogManager().Get(mockDatabase.DatabaseID, mockDatabase.DatabaseBranchID, app.Cluster.TieredFS()),
			config.StorageModeLocal,
			mockDatabase.DatabaseID,
			mockDatabase.DatabaseBranchID,
			4096,
		)

		drm := storage.NewDataRangeManager(dfs)
		dri := storage.NewDataRangeIndex(drm)

		ranges, err := dri.All()

		if err != nil {
			t.Errorf("Expected All to succeed, got error: %v", err)
		}

		if len(ranges) != 1 {
			t.Errorf("Expected All to return 1 entry, got %d entries", len(ranges))
		}

		dri.Set(2, 12345)
		dri.Set(3, 67890)

		ranges, err = dri.All()

		if err != nil {
			t.Errorf("Expected All to succeed, got error: %v", err)
		}

		if len(ranges) != 3 {
			t.Errorf("Expected All to return 3 entries, got %d", len(ranges))
		}

		if ranges[2].Version != 12345 {
			t.Errorf("Expected range 2 to have version 12345, got %d", ranges[2])
		}

		if ranges[3].Version != 67890 {
			t.Errorf("Expected range 3 to have version 67890, got %d", ranges[3])
		}
	})
}

func TestDataRangeIndex_Close(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mockDatabase := test.MockDatabase(app)

		dfs := storage.NewDurableDatabaseFileSystem(
			app.Cluster.TieredFS(),
			app.Cluster.NetworkFS(),
			app.DatabaseManager.PageLogManager().Get(mockDatabase.DatabaseID, mockDatabase.DatabaseBranchID, app.Cluster.TieredFS()),
			config.StorageModeLocal,
			mockDatabase.DatabaseID,
			mockDatabase.DatabaseBranchID,
			4096,
		)

		drm := storage.NewDataRangeManager(dfs)
		dri := storage.NewDataRangeIndex(drm)

		err := dri.Close()

		if err != nil {
			t.Errorf("Expected Close to succeed, got error: %v", err)
		}
	})
}

func TestDataRangeIndex_File(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mockDatabase := test.MockDatabase(app)

		dfs := storage.NewDurableDatabaseFileSystem(
			app.Cluster.TieredFS(),
			app.Cluster.NetworkFS(),
			app.DatabaseManager.PageLogManager().Get(mockDatabase.DatabaseID, mockDatabase.DatabaseBranchID, app.Cluster.TieredFS()),
			config.StorageModeLocal,
			mockDatabase.DatabaseID,
			mockDatabase.DatabaseBranchID,
			4096,
		)

		drm := storage.NewDataRangeManager(dfs)
		dri := storage.NewDataRangeIndex(drm)

		file, err := dri.File()

		if err != nil {
			t.Errorf("Expected File to succeed, got error: %v", err)
		}

		if file == nil {
			t.Error("Expected File to return a valid file, got nil")
		}

		// Clean up
		dri.Close()
	})
}

func TestDataRangeIndex_Get(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mockDatabase := test.MockDatabase(app)

		dfs := storage.NewDurableDatabaseFileSystem(
			app.Cluster.TieredFS(),
			app.Cluster.NetworkFS(),
			app.DatabaseManager.PageLogManager().Get(mockDatabase.DatabaseID, mockDatabase.DatabaseBranchID, app.Cluster.TieredFS()),
			config.StorageModeLocal,
			mockDatabase.DatabaseID,
			mockDatabase.DatabaseBranchID,
			4096,
		)

		drm := storage.NewDataRangeManager(dfs)
		dri := storage.NewDataRangeIndex(drm)

		found, version, err := dri.Get(1)

		if err != nil {
			t.Errorf("Expected Get to succeed, got error: %v", err)
		}

		if !found {
			t.Error("Expected Get to find the range, got not found")
		}

		if version <= 0 {
			t.Errorf("Expected version to be greater than 0, got %d", version)
		}

		found, version, err = dri.Get(2)

		if err != nil {
			t.Errorf("Expected Get to succeed, got error: %v", err)
		}

		if found {
			t.Error("Expected Get to not find the range, got found")
		}

		if version != 0 {
			t.Errorf("Expected version to be 0, got %d", version)
		}

		// Clean up
		dri.Close()
	})
}

func TestDataRangeIndex_Path(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mockDatabase := test.MockDatabase(app)

		dfs := storage.NewDurableDatabaseFileSystem(
			app.Cluster.TieredFS(),
			app.Cluster.NetworkFS(),
			app.DatabaseManager.PageLogManager().Get(mockDatabase.DatabaseID, mockDatabase.DatabaseBranchID, app.Cluster.TieredFS()),
			config.StorageModeLocal,
			mockDatabase.DatabaseID,
			mockDatabase.DatabaseBranchID,
			4096,
		)

		drm := storage.NewDataRangeManager(dfs)
		dri := storage.NewDataRangeIndex(drm)

		expectedPath := fmt.Sprintf("%s_RANGE_INDEX", file.GetDatabaseFileDir(mockDatabase.DatabaseID, mockDatabase.DatabaseBranchID))
		actualPath := dri.Path()

		if actualPath != expectedPath {
			t.Errorf("Expected Path to return %q, got %q", expectedPath, actualPath)
		}

		// Clean up
		dri.Close()
	})
}

func TestDataRangeIndex_Set(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mockDatabase := test.MockDatabase(app)

		dfs := storage.NewDurableDatabaseFileSystem(
			app.Cluster.TieredFS(),
			app.Cluster.NetworkFS(),
			app.DatabaseManager.PageLogManager().Get(mockDatabase.DatabaseID, mockDatabase.DatabaseBranchID, app.Cluster.TieredFS()),
			config.StorageModeLocal,
			mockDatabase.DatabaseID,
			mockDatabase.DatabaseBranchID,
			4096,
		)

		drm := storage.NewDataRangeManager(dfs)
		dri := storage.NewDataRangeIndex(drm)

		err := dri.Set(1, 12345)

		if err != nil {
			t.Errorf("Expected Set to succeed, got error: %v", err)
		}

		found, version, err := dri.Get(1)

		if err != nil {
			t.Errorf("Expected Get to succeed, got error: %v", err)
		}

		if !found {
			t.Error("Expected Get to find the range, got not found")
		}

		if version != 12345 {
			t.Errorf("Expected version to be 12345, got %d", version)
		}

		// Clean up
		dri.Close()
	})
}
