package storage_test

import (
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"
	testingStorage "github.com/litebase/litebase/internal/test/storage"
	"github.com/litebase/litebase/pkg/server"
)

func TestPageLoggerCompactionTimePersistence(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		t.Run("CompactionTimePersistsAcrossRestart", func(t *testing.T) {
			db := test.MockDatabase(app)

			// Create initial page logger
			pageLogger1, err := testingStorage.NewPageLoggerForTesting(
				db.DatabaseID,
				db.DatabaseBranchID,
				app.Cluster.LocalFS(),
			)

			if err != nil {
				t.Fatalf("Failed to create page logger: %v", err)
			}

			// Initially, CompactedAt should be zero
			if !pageLogger1.CompactedAt.IsZero() {
				t.Fatal("Expected initial CompactedAt to be zero")
			}

			// Write some test data
			testData := make([]byte, 4096)

			for i := range 100 {
				_, err := pageLogger1.Write(int64(i+1), int64(1), testData)

				if err != nil {
					t.Fatalf("Failed to write page: %v", err)
				}
			}

			// Perform compaction
			dfs := app.DatabaseManager.Resources(db.Branch).FileSystem()
			err = pageLogger1.Compact(dfs)

			if err != nil {
				t.Fatalf("Failed to compact: %v", err)
			}

			// Store the compaction time
			firstCompactionTime := pageLogger1.CompactedAt

			if firstCompactionTime.IsZero() {
				t.Fatal("Expected CompactedAt to be set after compaction")
			}

			// Close the first page logger
			err = pageLogger1.Close()

			if err != nil {
				t.Fatalf("Failed to close page logger: %v", err)
			}

			// Create a new page logger instance (simulating restart)
			pageLogger2, err := testingStorage.NewPageLoggerForTesting(
				db.DatabaseID,
				db.DatabaseBranchID,
				app.Cluster.LocalFS(),
			)

			if err != nil {
				t.Fatalf("Failed to create second page logger: %v", err)
			}

			defer func() {
				if err := pageLogger2.Close(); err != nil {
					t.Errorf("Failed to close second page logger: %v", err)
				}
			}()

			// Check that the compaction time was loaded from the index
			loadedCompactionTime := pageLogger2.CompactedAt

			if loadedCompactionTime.IsZero() {
				t.Fatal("Expected CompactedAt to be loaded from index after restart")
			}

			// The loaded time should match the original time (within nanosecond precision)
			if !loadedCompactionTime.Equal(firstCompactionTime) {
				t.Fatalf("Loaded compaction time %v does not match original %v",
					loadedCompactionTime, firstCompactionTime)
			}
		})

		t.Run("SetLastCompactionAtMethod", func(t *testing.T) {
			db := test.MockDatabase(app)

			pageLogger, err := testingStorage.NewPageLoggerForTesting(
				db.DatabaseID,
				db.DatabaseBranchID,
				app.Cluster.LocalFS(),
			)

			if err != nil {
				t.Fatalf("Failed to create page logger: %v", err)
			}

			defer func() {
				if err := pageLogger.Close(); err != nil {
					t.Errorf("Failed to close page logger: %v", err)
				}
			}()

			// Test the index setter/getter directly
			// Note: We can't access index directly, so we'll test through compaction

			// Write some data and compact to trigger SetLastCompactionAt
			testData := make([]byte, 4096)

			_, err = pageLogger.Write(1, 1, testData)

			if err != nil {
				t.Fatalf("Failed to write page: %v", err)
			}

			dfs := app.DatabaseManager.Resources(db.Branch).FileSystem()
			err = pageLogger.Compact(dfs)

			if err != nil {
				t.Fatalf("Failed to compact: %v", err)
			}

			// The CompactedAt should be close to now
			compactTime := pageLogger.CompactedAt

			if compactTime.IsZero() {
				t.Fatal("Expected CompactedAt to be set")
			}

			// Should be within the last few seconds
			if time.Since(compactTime) > 5*time.Second {
				t.Fatalf("CompactedAt time %v seems too old", compactTime)
			}
		})
	})
}
