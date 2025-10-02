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

func TestDataRangeIndex(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		t.Run("NewDataRangeIndex", func(t *testing.T) {
			dri := storage.NewDataRangeIndex(nil)

			if dri == nil {
				t.Error("Expected DataRangeIndex to be initialized")
			}
		})

		t.Run("All", func(t *testing.T) {
			mockDatabase := test.MockDatabase(app)

			dfs := storage.NewDurableDatabaseFileSystem(
				app.Cluster.TieredFS(),
				app.Cluster.NetworkFS(),
				app.DatabaseManager.PageLogManager().Get(mockDatabase.DatabaseID, mockDatabase.DatabaseBranchID, app.Cluster.NetworkFS()),
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

			if err := dri.Set(2, 12345); err != nil {
				t.Errorf("error setting range 2: %v", err)
			}

			if err := dri.Set(3, 67890); err != nil {
				t.Errorf("error setting range 3: %v", err)
			}

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

		t.Run("Close", func(t *testing.T) {

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

		t.Run("File", func(t *testing.T) {
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
			if err := dri.Close(); err != nil {
				t.Errorf("error closing data range index: %v", err)
			}
		})

		t.Run("Get", func(t *testing.T) {
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

			found, err := dri.Get(1)

			if err != nil {
				t.Errorf("Expected Get to succeed, got error: %v", err)
			}

			if !found {
				t.Error("Expected Get to find the range, got not found")
			}

			found, err = dri.Get(2)

			if err != nil {
				t.Errorf("Expected Get to succeed, got error: %v", err)
			}

			if found {
				t.Error("Expected Get to not find the range, got found")
			}

			// Clean up
			if err := dri.Close(); err != nil {
				t.Errorf("error closing data range index: %v", err)
			}
		})

		t.Run("Path", func(t *testing.T) {
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
			if err := dri.Close(); err != nil {
				t.Errorf("error closing data range index: %v", err)
			}
		})

		t.Run("Set", func(t *testing.T) {
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

			found, err := dri.Get(1)

			if err != nil {
				t.Errorf("Expected Get to succeed, got error: %v", err)
			}

			if !found {
				t.Error("Expected Get to find the range, got not found")
			}

			// Clean up
			if err := dri.Close(); err != nil {
				t.Errorf("error closing data range index: %v", err)
			}
		})
	})
}
