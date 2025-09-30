package storage_test

import (
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/server"
	"github.com/litebase/litebase/pkg/storage"
)

func TestPageLoggerIndex(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		t.Run("New", func(t *testing.T) {
			pli, err := storage.NewPageLoggerIndex(
				app.Cluster.NetworkFS(),
				"INDEX",
			)

			if err != nil {
				t.Fatal("expected no error, got:", err)
			}

			if pli == nil {
				t.Fatal("expected non-nil PageLoggerIndex")
			}
		})

		t.Run("Close", func(t *testing.T) {
			pli, err := storage.NewPageLoggerIndex(
				app.Cluster.NetworkFS(),
				"INDEX",
			)

			if err != nil {
				t.Fatal("expected no error, got:", err)
			}

			if err := pli.Close(); err != nil {
				t.Fatalf("unexpected error closing PageLoggerIndex: %v", err)
			}
		})

		t.Run("FileName", func(t *testing.T) {
			pli, err := storage.NewPageLoggerIndex(
				app.Cluster.NetworkFS(),
				"PLI_INDEX",
			)

			if err != nil {
				t.Fatal("expected no error, got:", err)
			}

			file := pli.File()

			if file == nil {
				t.Fatal("expected non-nil file, got nil")
			}

			if err := file.Close(); err != nil {
				t.Fatalf("unexpected error closing file: %v", err)
			}
		})

		t.Run("Find", func(t *testing.T) {
			pli, err := storage.NewPageLoggerIndex(
				app.Cluster.NetworkFS(),
				"FIND_INDEX",
			)

			if err != nil {
				t.Fatal("expected no error, got:", err)
			}

			err = pli.Push(storage.PageGroup(1), storage.PageNumber(1), storage.PageGroupVersion(1))

			if err != nil {
				t.Fatalf("unexpected error pushing page: %v", err)
			}

			_, found, err := pli.Find(storage.PageGroup(1), storage.PageNumber(1), storage.PageVersion(1))

			if err != nil {
				t.Fatalf("unexpected error finding page: %v", err)
			}

			if !found {
				t.Fatal("expected page to be found")
			}
		})

		t.Run("Push", func(t *testing.T) {
			pli, err := storage.NewPageLoggerIndex(
				app.Cluster.NetworkFS(),
				"INDEX",
			)

			if err != nil {
				t.Fatal("expected no error, got:", err)
			}

			err = pli.Push(storage.PageGroup(1), storage.PageNumber(1), storage.PageGroupVersion(1))

			if err != nil {
				t.Fatalf("unexpected error pushing page: %v", err)
			}
		})

		t.Run("ManyEntries", func(t *testing.T) {
			pli, err := storage.NewPageLoggerIndex(
				app.Cluster.NetworkFS(),
				"MANY_ENTRIES_INDEX",
			)

			if err != nil {
				t.Fatal("expected no error, got:", err)
			}

			// Add many entries across multiple page groups and versions
			// This tests the buffer management in the load() method
			const numPageGroups = 10
			const numVersionsPerGroup = 20
			const numPagesPerVersion = 50

			// Push a large number of entries
			for pageGroup := 1; pageGroup <= numPageGroups; pageGroup++ {
				for version := 1; version <= numVersionsPerGroup; version++ {
					for page := 1; page <= numPagesPerVersion; page++ {
						err = pli.Push(
							storage.PageGroup(pageGroup),
							storage.PageNumber(page),
							storage.PageGroupVersion(version),
						)

						if err != nil {
							t.Fatalf("unexpected error pushing page (group=%d, version=%d, page=%d): %v",
								pageGroup, version, page, err)
						}
					}
				}
			}

			// Close and reopen to test the load() method with many entries
			err = pli.Close()

			if err != nil {
				t.Fatalf("unexpected error closing PageLoggerIndex: %v", err)
			}

			// Reopen the index - this will trigger the load() method
			pli2, err := storage.NewPageLoggerIndex(
				app.Cluster.NetworkFS(),
				"MANY_ENTRIES_INDEX",
			)

			if err != nil {
				t.Fatalf("unexpected error reopening PageLoggerIndex: %v", err)
			}

			// Verify that we can find all the entries we added
			for pageGroup := 1; pageGroup <= numPageGroups; pageGroup++ {
				for version := 1; version <= numVersionsPerGroup; version++ {
					for page := 1; page <= numPagesPerVersion; page++ {
						foundVersion, found, err := pli2.Find(
							storage.PageGroup(pageGroup),
							storage.PageNumber(page),
							storage.PageVersion(version),
						)

						if err != nil {
							t.Fatalf("unexpected error finding page (group=%d, version=%d, page=%d): %v",
								pageGroup, version, page, err)
						}

						if !found {
							t.Fatalf("expected to find page (group=%d, version=%d, page=%d)",
								pageGroup, version, page)
						}

						if foundVersion != int64(version) {
							t.Fatalf("expected version %d, got %d for page (group=%d, page=%d)",
								version, foundVersion, pageGroup, page)
						}
					}
				}
			}

			// Test finding with version 0 (should find latest)
			foundVersion, found, err := pli2.Find(
				storage.PageGroup(1),
				storage.PageNumber(1),
				storage.PageVersion(0), // 0 means find latest
			)

			if err != nil {
				t.Fatalf("unexpected error finding latest version: %v", err)
			}

			if !found {
				t.Fatal("expected to find page with version 0 (latest)")
			}

			if foundVersion != numVersionsPerGroup {
				t.Fatalf("expected latest version %d, got %d", numVersionsPerGroup, foundVersion)
			}

			err = pli2.Close()

			if err != nil {
				t.Fatalf("unexpected error closing PageLoggerIndex: %v", err)
			}
		})

		t.Run("LoadCorruptedData", func(t *testing.T) {
			pli, err := storage.NewPageLoggerIndex(
				app.Cluster.NetworkFS(),
				"CORRUPTED_INDEX",
			)

			if err != nil {
				t.Fatal("expected no error, got:", err)
			}

			// Add some entries first
			err = pli.Push(storage.PageGroup(1), storage.PageNumber(1), storage.PageGroupVersion(1))

			if err != nil {
				t.Fatalf("unexpected error pushing page: %v", err)
			}

			err = pli.Push(storage.PageGroup(2), storage.PageNumber(2), storage.PageGroupVersion(2))

			if err != nil {
				t.Fatalf("unexpected error pushing page: %v", err)
			}

			err = pli.Close()

			if err != nil {
				t.Fatalf("unexpected error closing PageLoggerIndex: %v", err)
			}

			// Try to reopen - this should work with valid data
			pli2, err := storage.NewPageLoggerIndex(
				app.Cluster.NetworkFS(),
				"CORRUPTED_INDEX",
			)

			if err != nil {
				t.Fatalf("unexpected error reopening PageLoggerIndex: %v", err)
			}

			// Verify the data is still accessible
			_, found, err := pli2.Find(storage.PageGroup(1), storage.PageNumber(1), storage.PageVersion(1))
			if err != nil {
				t.Fatalf("unexpected error finding page: %v", err)
			}

			if !found {
				t.Fatal("expected to find page after reload")
			}

			err = pli2.Close()

			if err != nil {
				t.Fatalf("unexpected error closing PageLoggerIndex: %v", err)
			}
		})

		t.Run("LastCompactionAtPersistence", func(t *testing.T) {
			pli, err := storage.NewPageLoggerIndex(
				app.Cluster.NetworkFS(),
				"COMPACTION_TIME_INDEX",
			)

			if err != nil {
				t.Fatal("expected no error, got:", err)
			}

			// Initially, lastCompactionAt should be zero
			initialTime := pli.GetLastCompactionAt()

			if !initialTime.IsZero() {
				t.Fatalf("expected initial lastCompactionAt to be zero, got: %v", initialTime)
			}

			// Set a specific compaction time
			testTime := time.Now().UTC().Truncate(time.Nanosecond)
			err = pli.SetLastCompactionAt(testTime)

			if err != nil {
				t.Fatalf("unexpected error setting lastCompactionAt: %v", err)
			}

			// Verify it was set correctly
			retrievedTime := pli.GetLastCompactionAt()

			if !retrievedTime.Equal(testTime) {
				t.Fatalf("expected lastCompactionAt %v, got %v", testTime, retrievedTime)
			}

			// Close and reopen to test persistence
			err = pli.Close()

			if err != nil {
				t.Fatalf("unexpected error closing PageLoggerIndex: %v", err)
			}

			// Reopen the index
			pli2, err := storage.NewPageLoggerIndex(
				app.Cluster.NetworkFS(),
				"COMPACTION_TIME_INDEX",
			)

			if err != nil {
				t.Fatalf("unexpected error reopening PageLoggerIndex: %v", err)
			}

			// Verify that the lastCompactionAt was persisted and loaded correctly
			loadedTime := pli2.GetLastCompactionAt()

			if !loadedTime.Equal(testTime) {
				t.Fatalf("expected loaded lastCompactionAt %v, got %v", testTime, loadedTime)
			}

			// Test setting to zero time (clearing the timestamp)
			err = pli2.SetLastCompactionAt(time.Time{})

			if err != nil {
				t.Fatalf("unexpected error setting lastCompactionAt to zero: %v", err)
			}

			zeroTime := pli2.GetLastCompactionAt()

			if !zeroTime.IsZero() {
				t.Fatalf("expected lastCompactionAt to be zero after setting to zero time, got: %v", zeroTime)
			}

			// Close and reopen again to test zero time persistence
			err = pli2.Close()

			if err != nil {
				t.Fatalf("unexpected error closing PageLoggerIndex: %v", err)
			}

			pli3, err := storage.NewPageLoggerIndex(
				app.Cluster.NetworkFS(),
				"COMPACTION_TIME_INDEX",
			)

			if err != nil {
				t.Fatalf("unexpected error reopening PageLoggerIndex: %v", err)
			}

			// Verify zero time was persisted
			finalTime := pli3.GetLastCompactionAt()

			if !finalTime.IsZero() {
				t.Fatalf("expected final lastCompactionAt to be zero after reload, got: %v", finalTime)
			}

			err = pli3.Close()

			if err != nil {
				t.Fatalf("unexpected error closing PageLoggerIndex: %v", err)
			}
		})
	})
}
