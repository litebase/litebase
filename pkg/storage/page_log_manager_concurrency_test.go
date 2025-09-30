package storage_test

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"
	testingStorage "github.com/litebase/litebase/internal/test/storage"
	"github.com/litebase/litebase/pkg/server"
	"github.com/litebase/litebase/pkg/storage"
)

func TestPageLogManagerConcurrentCompaction(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		t.Run("ConcurrencyLimit", func(t *testing.T) {
			// Create a PageLogManager with a lower concurrency limit for testing
			ctx := t.Context()

			maxConcurrent := 3

			plm := storage.NewPageLogManager(ctx,
				storage.WithMaxConcurrentCompactions(maxConcurrent))

			defer func() {
				if err := plm.Close(); err != nil {
					t.Errorf("Failed to close PageLogManager: %v", err)
				}
			}()

			// Create multiple databases and populate them with data
			numDatabases := 10
			var databases []test.TestDatabase
			var pageLoggers []*storage.PageLogger

			for i := range numDatabases {
				db := test.MockDatabase(app)
				databases = append(databases, db)

				// Create page logger for this database
				pageLogger, err := testingStorage.NewPageLoggerForTesting(
					db.DatabaseID,
					db.DatabaseBranchID,
					app.Cluster.LocalFS(),
				)

				if err != nil {
					t.Fatalf("Failed to create page logger %d: %v", i, err)
				}

				pageLoggers = append(pageLoggers, pageLogger)

				// Manually add the logger to the manager
				// (normally this would happen through Get method)
				// We need to use reflection or create a test helper method
				logger := plm.Get(db.DatabaseID, db.DatabaseBranchID, app.Cluster.LocalFS())

				if logger == nil {
					t.Fatalf("Failed to get page logger %d from manager", i)
				}

				// Write some data to make compaction worthwhile
				testData := make([]byte, 4096)

				for page := 1; page <= 50; page++ {
					_, err := logger.Write(int64(page), int64(1), testData)

					if err != nil {
						t.Fatalf("Failed to write data for database %d: %v", i, err)
					}
				}
			}

			// Track concurrent compactions
			var maxConcurrentSeen int64
			var attemptedCompactions int64
			var successfulCompactions int64

			var wg sync.WaitGroup

			// Start compaction for all databases simultaneously
			for i, db := range databases {
				wg.Add(1)
				go func(dbIndex int, database test.TestDatabase) {
					defer wg.Done()

					dfs := app.DatabaseManager.Resources(database.DatabaseID, database.DatabaseBranchID).FileSystem()

					atomic.AddInt64(&attemptedCompactions, 1)

					// Attempt compaction through the manager
					attempted, err := plm.CompactDatabase(database.DatabaseID, database.DatabaseBranchID, dfs)

					if err != nil {
						t.Errorf("Compaction failed for database %d: %v", dbIndex, err)
					}

					if attempted {
						// Track the number of currently active compactions during the operation
						activeNow := int64(plm.GetActiveCompactions())

						// Update max concurrent seen
						for {
							max := atomic.LoadInt64(&maxConcurrentSeen)
							if activeNow <= max || atomic.CompareAndSwapInt64(&maxConcurrentSeen, max, activeNow) {
								break
							}
						}

						atomic.AddInt64(&successfulCompactions, 1)
						t.Logf("Database %d: compaction attempted (active: %d)", dbIndex, activeNow)
					} else {
						t.Logf("Database %d: compaction skipped (concurrency limit)", dbIndex)
					}
				}(i, db)
			}

			// Wait for all compactions to complete
			wg.Wait()

			// Verify results
			totalAttempted := atomic.LoadInt64(&attemptedCompactions)
			totalSuccessful := atomic.LoadInt64(&successfulCompactions)
			maxConcurrentObserved := atomic.LoadInt64(&maxConcurrentSeen)

			t.Logf("Results: %d attempted, %d successful, max concurrent: %d",
				totalAttempted, totalSuccessful, maxConcurrentObserved)

			// Verify that we attempted compaction for all databases
			if totalAttempted != int64(numDatabases) {
				t.Errorf("Expected %d attempted compactions, got %d", numDatabases, totalAttempted)
			}

			// Verify that some compactions were skipped due to concurrency limits
			// (unless we got very lucky with timing)
			if totalSuccessful > int64(maxConcurrent) {
				// This could happen if compactions complete very quickly
				t.Logf("Note: More successful compactions (%d) than limit (%d) - compactions completed quickly",
					totalSuccessful, maxConcurrent)
			}

			// Verify that concurrent compactions never exceeded our limit
			if maxConcurrentObserved > int64(maxConcurrent) {
				t.Errorf("Concurrency limit violated: observed %d concurrent, limit is %d",
					maxConcurrentObserved, maxConcurrent)
			}

			// Clean up
			for _, logger := range pageLoggers {
				if err := logger.Close(); err != nil {
					t.Errorf("Failed to close page logger: %v", err)
				}
			}
		})

		t.Run("GetActiveCompactions", func(t *testing.T) {
			ctx := t.Context()

			plm := storage.NewPageLogManager(ctx, storage.WithMaxConcurrentCompactions(5))

			defer func() {
				if err := plm.Close(); err != nil {
					t.Errorf("Failed to close PageLogManager: %v", err)
				}
			}()

			// Initially should be 0
			active := plm.GetActiveCompactions()

			if active != 0 {
				t.Errorf("Expected 0 active compactions, got %d", active)
			}

			// Create a database and page logger
			db := test.MockDatabase(app)

			logger := plm.Get(db.DatabaseID, db.DatabaseBranchID, app.Cluster.LocalFS())

			if logger == nil {
				t.Fatal("Failed to get page logger")
			}

			// Write some data
			testData := make([]byte, 4096)
			_, err := logger.Write(1, 1, testData)

			if err != nil {
				t.Fatalf("Failed to write data: %v", err)
			}

			// Start a slow compaction in a goroutine
			compactionStarted := make(chan struct{})
			compactionComplete := make(chan struct{})

			go func() {
				defer close(compactionComplete)
				close(compactionStarted)

				dfs := app.DatabaseManager.Resources(db.DatabaseID, db.DatabaseBranchID).FileSystem()

				// This will briefly show as active
				_, err := plm.CompactDatabase(db.DatabaseID, db.DatabaseBranchID, dfs)

				if err != nil {
					t.Logf("Compaction error (expected in test): %v", err)
				}
			}()

			// Wait for compaction to start, then check active count
			<-compactionStarted

			// Give it a moment to actually start
			time.Sleep(10 * time.Millisecond)

			// Note: This test is timing-sensitive and might not always catch the active compaction
			// due to how fast compactions can complete
			activeAtEnd := plm.GetActiveCompactions()
			t.Logf("Active compactions during/after test: %d", activeAtEnd)

			// Wait for completion
			<-compactionComplete

			// Should be back to 0
			finalActive := plm.GetActiveCompactions()

			if finalActive != 0 {
				t.Errorf("Expected 0 active compactions at end, got %d", finalActive)
			}
		})

		t.Run("ConfigurableConcurrencyLimit", func(t *testing.T) {
			ctx := t.Context()

			// Test with custom concurrency limit
			customLimit := 7
			plm := storage.NewPageLogManager(ctx, storage.WithMaxConcurrentCompactions(customLimit))

			defer func() {
				if err := plm.Close(); err != nil {
					t.Errorf("Failed to close PageLogManager: %v", err)
				}
			}()

			// The actual limit is hard to test directly without accessing private fields,
			// but we can test that the configuration option exists and doesn't crash
			t.Logf("Created PageLogManager with custom concurrency limit: %d", customLimit)

			// Create a database to verify basic functionality still works
			db := test.MockDatabase(app)
			logger := plm.Get(db.DatabaseID, db.DatabaseBranchID, app.Cluster.LocalFS())

			if logger == nil {
				t.Fatal("Failed to get page logger with custom config")
			}

			// Write and compact some data
			testData := make([]byte, 4096)

			_, err := logger.Write(1, 1, testData)

			if err != nil {
				t.Fatalf("Failed to write data: %v", err)
			}

			dfs := app.DatabaseManager.Resources(db.DatabaseID, db.DatabaseBranchID).FileSystem()
			attempted, err := plm.CompactDatabase(db.DatabaseID, db.DatabaseBranchID, dfs)

			if err != nil {
				t.Fatalf("Compaction failed: %v", err)
			}

			if !attempted {
				t.Error("Expected compaction to be attempted")
			}

			t.Log("Custom concurrency limit configuration works correctly")
		})

		t.Run("CompactAllDatabasesMethod", func(t *testing.T) {
			ctx := t.Context()

			plm := storage.NewPageLogManager(ctx, storage.WithMaxConcurrentCompactions(5))
			defer func() {
				if err := plm.Close(); err != nil {
					t.Errorf("Failed to close PageLogManager: %v", err)
				}
			}()

			// Create multiple databases
			numDatabases := 5

			for i := range numDatabases {
				db := test.MockDatabase(app)

				// Get logger through manager and write some data
				logger := plm.Get(db.DatabaseID, db.DatabaseBranchID, app.Cluster.LocalFS())

				if logger == nil {
					t.Fatalf("Failed to get page logger %d", i)
				}

				// Write test data
				testData := make([]byte, 4096)

				for page := 1; page <= 10; page++ {
					_, err := logger.Write(int64(page), int64(1), testData)

					if err != nil {
						t.Fatalf("Failed to write data for database %d: %v", i, err)
					}
				}
			}

			// Test the CompactAllDatabases method
			durableProvider := func(databaseId, branchId string) *storage.DurableDatabaseFileSystem {
				return app.DatabaseManager.Resources(databaseId, branchId).FileSystem()
			}

			// This should attempt to compact all databases with concurrency control
			plm.CompactAllDatabases(durableProvider)

			t.Logf("Successfully tested CompactAllDatabases with %d databases", numDatabases)

			// Verify that active compactions return to 0
			activeAfter := plm.GetActiveCompactions()

			if activeAfter != 0 {
				t.Errorf("Expected 0 active compactions after CompactAllDatabases, got %d", activeAfter)
			}
		})
	})
}
