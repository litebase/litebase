package storage_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/database"
	"github.com/litebase/litebase/pkg/file"
	"github.com/litebase/litebase/pkg/server"
)

// TestPageLogger_OrphanedCleanup verifies that orphaned page logs (files that exist
// on disk but are not referenced in the index) are automatically cleaned up when
// the PageLogger is initialized.
func TestPageLogger_OrphanedCleanup(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		// Create a database and branch
		db, err := database.CreateDatabase(app.DatabaseManager, "orphaned_test", "main")

		if err != nil {
			t.Fatalf("Failed to create database: %v", err)
		}

		branch, err := db.Branch("main")

		if err != nil {
			t.Fatalf("Failed to get branch: %v", err)
		}

		// Get the page logger
		resources := app.DatabaseManager.Resources(branch)
		pageLogger := resources.FileSystem().PageLogger

		// Write some pages to create page logs
		timestamp1 := time.Now().UTC().UnixNano()
		data := make([]byte, 4096)

		for i := 0; i < 100; i++ {
			_, err = pageLogger.Write(int64(i+1), timestamp1, data)

			if err != nil {
				t.Fatalf("Failed to write page: %v", err)
			}
		}

		err = pageLogger.Sync()

		if err != nil {
			t.Fatalf("Failed to sync page logger: %v", err)
		}

		// Create another set of page logs with a different timestamp
		timestamp2 := time.Now().UTC().UnixNano()

		for i := 0; i < 100; i++ {
			_, err = pageLogger.Write(int64(i+1), timestamp2, data)

			if err != nil {
				t.Fatalf("Failed to write page: %v", err)
			}
		}

		err = pageLogger.Sync()

		if err != nil {
			t.Fatalf("Failed to sync page logger: %v", err)
		}

		// Count initial page logs
		logDir := fmt.Sprintf("%slogs/page/", file.GetDatabaseFileBaseDir(db.DatabaseID, branch.DatabaseBranchID))
		initialFiles, err := app.Cluster.NetworkFS().ReadDir(logDir)

		if err != nil {
			t.Fatalf("Failed to read log directory: %v", err)
		}

		initialPageLogCount := 0

		for _, f := range initialFiles {
			if !f.IsDir() && len(f.Name()) > 9 && f.Name()[:9] == "PAGE_LOG_" {
				initialPageLogCount++
			}
		}

		if initialPageLogCount == 0 {
			t.Fatal("Expected at least some page logs to be created")
		}

		t.Logf("Created %d page logs initially", initialPageLogCount)

		// Close the page logger
		err = pageLogger.Close()

		if err != nil {
			t.Fatalf("Failed to close page logger: %v", err)
		}

		// Manually delete some entries from the index to simulate orphaned page logs
		// This simulates what happened with the bug where empty page logs were removed
		// from the index but not deleted from disk
		indexPath := fmt.Sprintf("%slogs/page/PAGE_LOGGER_INDEX", file.GetDatabaseFileBaseDir(db.DatabaseID, branch.DatabaseBranchID))
		err = app.Cluster.NetworkFS().Remove(indexPath)

		if err != nil {
			t.Fatalf("Failed to remove index: %v", err)
		}

		t.Logf("Removed index file to simulate orphaned page logs")

		// Reinitialize the database resources - this should trigger cleanup of orphaned files
		// when the PageLogger is recreated
		err = app.DatabaseManager.ShutdownResources()

		if err != nil {
			t.Fatalf("Failed to shutdown resources: %v", err)
		}

		// Get new resources which will create a fresh PageLogger
		newResources := app.DatabaseManager.Resources(branch)
		newPageLogger := newResources.FileSystem().PageLogger

		// Count page logs after cleanup
		filesAfterCleanup, err := app.Cluster.NetworkFS().ReadDir(logDir)

		if err != nil {
			t.Fatalf("Failed to read log directory after cleanup: %v", err)
		}

		pageLogCountAfterCleanup := 0

		for _, f := range filesAfterCleanup {
			if !f.IsDir() && len(f.Name()) > 9 && f.Name()[:9] == "PAGE_LOG_" {
				pageLogCountAfterCleanup++
			}
		}

		t.Logf("Page logs after cleanup: %d", pageLogCountAfterCleanup)

		// With no index, all page logs should be considered orphaned and removed
		if pageLogCountAfterCleanup != 0 {
			t.Errorf("Expected all page logs to be cleaned up, but found %d remaining", pageLogCountAfterCleanup)
		}

		// Close the new page logger
		err = newPageLogger.Close()

		if err != nil {
			t.Fatalf("Failed to close new page logger: %v", err)
		}

		t.Log("Orphaned page log cleanup test completed successfully")
	})
}

// TestPageLogger_OrphanedCleanup_PartialOrphans verifies that cleanup only removes
// orphaned page logs and preserves page logs that are still referenced in the index.
func TestPageLogger_OrphanedCleanup_PartialOrphans(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		// Create a database and branch
		db, err := database.CreateDatabase(app.DatabaseManager, "partial_orphaned_test", "main")

		if err != nil {
			t.Fatalf("Failed to create database: %v", err)
		}

		branch, err := db.Branch("main")

		if err != nil {
			t.Fatalf("Failed to get branch: %v", err)
		}

		// Get the page logger
		resources := app.DatabaseManager.Resources(branch)
		pageLogger := resources.FileSystem().PageLogger

		// Write pages to create page logs
		timestamp := time.Now().UTC().UnixNano()
		data := make([]byte, 4096)

		for i := 0; i < 100; i++ {
			_, err = pageLogger.Write(int64(i+1), timestamp, data)

			if err != nil {
				t.Fatalf("Failed to write page: %v", err)
			}
		}

		err = pageLogger.Sync()

		if err != nil {
			t.Fatalf("Failed to sync page logger: %v", err)
		}

		// Close the page logger
		err = pageLogger.Close()

		if err != nil {
			t.Fatalf("Failed to close page logger: %v", err)
		}

		// Create an orphaned page log by manually writing a file that's not in the index
		logDir := fmt.Sprintf("%slogs/page/", file.GetDatabaseFileBaseDir(db.DatabaseID, branch.DatabaseBranchID))
		orphanedLogPath := fmt.Sprintf("%sPAGE_LOG_999_123456789", logDir)
		orphanedData := []byte("orphaned data")

		err = app.Cluster.NetworkFS().WriteFile(orphanedLogPath, orphanedData, 0600)

		if err != nil {
			t.Fatalf("Failed to create orphaned page log: %v", err)
		}

		t.Log("Created orphaned page log file")

		// Count page logs before cleanup
		filesBefore, err := app.Cluster.NetworkFS().ReadDir(logDir)

		if err != nil {
			t.Fatalf("Failed to read log directory: %v", err)
		}

		pageLogCountBefore := 0

		for _, f := range filesBefore {
			if !f.IsDir() && len(f.Name()) > 9 && f.Name()[:9] == "PAGE_LOG_" {
				pageLogCountBefore++
			}
		}

		t.Logf("Page logs before cleanup: %d", pageLogCountBefore)

		// Reinitialize the database resources - this should trigger cleanup of orphaned files
		err = app.DatabaseManager.ShutdownResources()

		if err != nil {
			t.Fatalf("Failed to shutdown resources: %v", err)
		}

		// Get new resources which will create a fresh PageLogger
		newResources := app.DatabaseManager.Resources(branch)
		newPageLogger := newResources.FileSystem().PageLogger

		// Count page logs after cleanup
		filesAfter, err := app.Cluster.NetworkFS().ReadDir(logDir)

		if err != nil {
			t.Fatalf("Failed to read log directory after cleanup: %v", err)
		}

		pageLogCountAfter := 0
		orphanedLogExists := false

		for _, f := range filesAfter {
			if !f.IsDir() && len(f.Name()) > 9 && f.Name()[:9] == "PAGE_LOG_" {
				pageLogCountAfter++

				if f.Name() == "PAGE_LOG_999_123456789" {
					orphanedLogExists = true
				}
			}
		}

		t.Logf("Page logs after cleanup: %d", pageLogCountAfter)

		// The orphaned file should have been removed
		if orphanedLogExists {
			t.Error("Expected orphaned page log to be removed, but it still exists")
		}

		// The legitimate page logs should still exist
		expectedCount := pageLogCountBefore - 1 // minus the orphaned one

		if pageLogCountAfter != expectedCount {
			t.Errorf("Expected %d page logs after cleanup, but found %d", expectedCount, pageLogCountAfter)
		}

		// Close the new page logger
		err = newPageLogger.Close()

		if err != nil {
			t.Fatalf("Failed to close new page logger: %v", err)
		}

		t.Log("Partial orphaned page log cleanup test completed successfully")
	})
}
