package database_test

import (
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/database"
	"github.com/litebase/litebase/pkg/server"
)

func TestDatabaseExport(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		t.Run("NewDatabaseExport", func(t *testing.T) {
			mock := test.MockDatabase(app)

			dfs := app.DatabaseManager.Resources(mock.DatabaseID, mock.DatabaseBranchID).FileSystem()
			ranges, err := dfs.RangeManager.Index.All()

			if err != nil {
				t.Fatal(err)
			}

			export := database.NewDatabaseExport(dfs, ranges)

			if export == nil {
				t.Fatal("Expected export to be non-nil")
			}

			if export.ID == "" {
				t.Fatal("Expected export to have an ID")
			}

			if export.StartedAt.IsZero() {
				t.Fatal("Expected export to have a start time")
			}

			if export.CompletedAt != nil {
				t.Fatal("Expected export to not have a completion time yet")
			}
		})

		t.Run("RangeCount", func(t *testing.T) {
			mock := test.MockDatabase(app)

			dfs := app.DatabaseManager.Resources(mock.DatabaseID, mock.DatabaseBranchID).FileSystem()
			ranges, err := dfs.RangeManager.Index.All()

			if err != nil {
				t.Fatal(err)
			}

			export := database.NewDatabaseExport(dfs, ranges)

			rangeCount := export.RangeCount()

			if rangeCount == 0 {
				t.Fatal("Expected at least one range")
			}

			if rangeCount != len(ranges) {
				t.Fatalf("Expected range count to be %d, got %d", len(ranges), rangeCount)
			}
		})

		t.Run("GetRange", func(t *testing.T) {
			mock := test.MockDatabase(app)

			dfs := app.DatabaseManager.Resources(mock.DatabaseID, mock.DatabaseBranchID).FileSystem()
			ranges, err := dfs.RangeManager.Index.All()

			if err != nil {
				t.Fatal(err)
			}

			export := database.NewDatabaseExport(dfs, ranges)

			// Get the first range
			rangeNumber := int64(1)
			rangeFile, err := export.GetRange(rangeNumber)

			if err != nil {
				t.Fatalf("Expected no error getting range %d, got %v", rangeNumber, err)
			}

			if rangeFile == nil {
				t.Fatalf("Expected range file to be non-nil for range %d", rangeNumber)
			}
		})

		t.Run("GetRange_NotFound", func(t *testing.T) {
			mock := test.MockDatabase(app)

			dfs := app.DatabaseManager.Resources(mock.DatabaseID, mock.DatabaseBranchID).FileSystem()
			ranges, err := dfs.RangeManager.Index.All()

			if err != nil {
				t.Fatal(err)
			}

			export := database.NewDatabaseExport(dfs, ranges)

			// Try to get a non-existent range
			rangeNumber := int64(999999)
			_, err = export.GetRange(rangeNumber)

			if err == nil {
				t.Fatal("Expected error getting non-existent range")
			}

			expectedError := "range not found in export"

			if err.Error() != expectedError {
				t.Fatalf("Expected error '%s', got '%s'", expectedError, err.Error())
			}
		})

		t.Run("End", func(t *testing.T) {
			mock := test.MockDatabase(app)

			dfs := app.DatabaseManager.Resources(mock.DatabaseID, mock.DatabaseBranchID).FileSystem()
			ranges, err := dfs.RangeManager.Index.All()

			if err != nil {
				t.Fatal(err)
			}

			export := database.NewDatabaseExport(dfs, ranges)

			if export.CompletedAt != nil {
				t.Fatal("Expected export to not have a completion time yet")
			}

			// Wait a small amount to ensure time difference
			time.Sleep(10 * time.Millisecond)

			export.End()

			if export.CompletedAt == nil {
				t.Fatal("Expected export to have a completion time after End()")
			}

			if !export.CompletedAt.After(export.StartedAt) {
				t.Fatal("Expected completion time to be after start time")
			}
		})
	})
}
