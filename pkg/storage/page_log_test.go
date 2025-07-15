package storage_test

import (
	"bytes"
	"crypto/rand"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/server"
	"github.com/litebase/litebase/pkg/storage"
)

func TestNewPageLog(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		pageLog, err := storage.NewPageLog(
			app.Cluster.TieredFS(),
			"PAGE_LOG",
		)

		if err != nil {
			t.Fatalf("Failed to create new page log: %v", err)
		}

		if pageLog == nil {
			t.Fatal("Expected page log to be created, but got nil")
		}
	})
}

func TestPageLog_Append(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		pageLog, err := storage.NewPageLog(
			app.Cluster.TieredFS(),
			"PAGE_LOG",
		)

		if err != nil {
			t.Fatalf("Failed to create new page log: %v", err)
		}

		testCases := []struct {
			pageNum   int64
			version   int64
			data      []byte
			expectErr bool
		}{
			{1, 1, make([]byte, 4096), false},
			{1, 2, make([]byte, 4096), false},
			{1, 4, make([]byte, 4096), false},
			{1, 1, make([]byte, 4095), true},
		}

		for _, tc := range testCases {
			rand.Read(tc.data)

			err := pageLog.Append(tc.pageNum, tc.version, tc.data)

			if (err != nil) != tc.expectErr {
				t.Fatalf("Expected error: %v, got: %v", tc.expectErr, err)
			}
		}
	})
}

func TestPageLog_Close(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		pageLog, err := storage.NewPageLog(
			app.Cluster.TieredFS(),
			"PAGE_LOG",
		)

		if err != nil {
			t.Fatalf("Failed to create new page log: %v", err)
		}

		err = pageLog.Close()

		if err != nil {
			t.Fatalf("Failed to close page log: %v", err)
		}
	})
}

func TestPageLog_Delete(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		fileSystem := app.Cluster.TieredFS()

		pageLog, err := storage.NewPageLog(
			fileSystem,
			"PAGE_LOG",
		)

		if err != nil {
			t.Fatalf("Failed to create new page log: %v", err)
		}

		file := pageLog.File()

		if file == nil {
			t.Fatal("Expected page log file to be created, but got nil")
		}

		stat, err := fileSystem.Stat(pageLog.Path)

		if err != nil {
			t.Fatalf("Failed to stat page log: %v", err)
		}

		if stat == nil {
			t.Fatal("Expected page log file to exist, but got nil")
		}

		err = pageLog.Delete()

		if err != nil {
			t.Fatalf("Failed to delete page log: %v", err)
		}

		stat, err = fileSystem.Stat(pageLog.Path)

		if err == nil {
			t.Fatal("Expected page log file to be deleted, but it still exists")
		}

		if stat != nil {
			t.Fatalf("Expected page log file to be nil, but got: %v", stat)
		}
	})
}

func TestPageLog_File(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		pageLog, err := storage.NewPageLog(
			app.Cluster.TieredFS(),
			"PAGE_LOG",
		)

		if err != nil {
			t.Fatalf("Failed to create new page log: %v", err)
		}

		file := pageLog.File()

		if file == nil {
			t.Fatal("Expected page log file to be created, but got nil")
		}
	})
}

func TestPageLog_Get(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		pageLog, err := storage.NewPageLog(
			app.Cluster.TieredFS(),
			"PAGE_LOG",
		)

		if err != nil {
			t.Fatalf("Failed to create new page log: %v", err)
		}

		testCases := []struct {
			pageNum int64
			version int64
			data    []byte
		}{
			{1, 1, make([]byte, 4096)},
			{1, 2, make([]byte, 4096)},
			{1, 4, make([]byte, 4096)},
		}

		for _, tc := range testCases {
			rand.Read(tc.data)

			err := pageLog.Append(tc.pageNum, tc.version, tc.data)

			if err != nil {
				t.Fatalf("Failed to append data: %v", err)
			}
		}

		data := make([]byte, 4096)

		found, foundVersion, err := pageLog.Get(1, 3, data)

		if err != nil {
			t.Fatalf("Failed to get data: %v", err)
		}

		if !found {
			t.Fatal("Expected data not found")
		}

		if foundVersion != 2 {
			t.Fatal("Expected found version does not match")
		}

		if !bytes.Equal(data, testCases[1].data) {
			t.Fatal("Expected data does not match")
		}
	})
}

func TestPageLog_RestoresAfterClose(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		pageLog, err := storage.NewPageLog(
			app.Cluster.TieredFS(),
			"PAGE_LOG",
		)

		if err != nil {
			t.Fatalf("Failed to create new page log: %v", err)
		}

		data := make([]byte, 4096)

		rand.Read(data)

		err = pageLog.Append(1, 1, data)

		if err != nil {
			t.Fatalf("Failed to append data: %v", err)
		}

		err = pageLog.Close()

		if err != nil {
			t.Fatalf("Failed to close page log: %v", err)
		}

		pageLog, err = storage.NewPageLog(
			app.Cluster.TieredFS(),
			"PAGE_LOG",
		)

		if err != nil {
			t.Fatalf("Failed to reopen page log: %v", err)
		}

		retrievedData := make([]byte, 4096)

		found, foundVersion, err := pageLog.Get(1, 1, retrievedData)

		if err != nil {
			t.Fatalf("Failed to get data after reopening: %v", err)
		}

		if !found {
			t.Fatal("Expected data not found after reopening")
		}

		if foundVersion != 1 {
			t.Fatal("Expected found version does not match")
		}

		if !bytes.Equal(retrievedData, data) {
			t.Fatal("Expected data does not match after reopening")
		}
	})
}

func TestPageLog_Sync(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		pageLog, err := storage.NewPageLog(
			app.Cluster.TieredFS(),
			"PAGE_LOG",
		)

		if err != nil {
			t.Fatalf("Failed to create new page log: %v", err)
		}

		// Test syncing empty page log
		err = pageLog.Sync()

		if err != nil {
			t.Fatalf("Failed to sync empty page log: %v", err)
		}

		// Test syncing after appending data
		data := make([]byte, 4096)
		rand.Read(data)

		err = pageLog.Append(1, 1, data)

		if err != nil {
			t.Fatalf("Failed to append data: %v", err)
		}

		err = pageLog.Sync()

		if err != nil {
			t.Fatalf("Failed to sync page log after append: %v", err)
		}

		// Test syncing multiple times
		err = pageLog.Sync()

		if err != nil {
			t.Fatalf("Failed to sync page log multiple times: %v", err)
		}

		// Test syncing with multiple pages
		for i := int64(2); i <= 5; i++ {
			data := make([]byte, 4096)
			rand.Read(data)

			err = pageLog.Append(i, 1, data)
			if err != nil {
				t.Fatalf("Failed to append data for page %d: %v", i, err)
			}
		}

		err = pageLog.Sync()

		if err != nil {
			t.Fatalf("Failed to sync page log with multiple pages: %v", err)
		}

		// Verify data persists after sync by reading it back
		readData := make([]byte, 4096)

		found, foundVersion, err := pageLog.Get(1, 1, readData)

		if err != nil {
			t.Fatalf("Failed to read data after sync: %v", err)
		}

		if !found {
			t.Fatal("Data not found after sync")
		}

		if foundVersion != 1 {
			t.Fatalf("Expected version 1, got %d", foundVersion)
		}

		if !bytes.Equal(data, readData) {
			t.Fatal("Data mismatch after sync")
		}
	})
}

func TestPageLog_SyncAfterDelete(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		pageLog, err := storage.NewPageLog(
			app.Cluster.TieredFS(),
			"PAGE_LOG",
		)

		if err != nil {
			t.Fatalf("Failed to create new page log: %v", err)
		}

		// Add some data
		data := make([]byte, 4096)
		rand.Read(data)

		err = pageLog.Append(1, 1, data)
		if err != nil {
			t.Fatalf("Failed to append data: %v", err)
		}

		// Delete the page log
		err = pageLog.Delete()

		if err != nil {
			t.Fatalf("Failed to delete page log: %v", err)
		}

		// Try to sync after delete - should return error
		err = pageLog.Sync()

		if err == nil {
			t.Fatal("Expected error when syncing deleted page log, but got nil")
		}

		expectedError := "cannot sync a deleted page log"

		if err.Error() != expectedError {
			t.Fatalf("Expected error '%s', got '%s'", expectedError, err.Error())
		}
	})
}

func TestPageLog_Tombstone(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		pageLog, err := storage.NewPageLog(
			app.Cluster.TieredFS(),
			"PAGE_LOG",
		)

		if err != nil {
			t.Fatalf("Failed to create new page log: %v", err)
		}

		data := make([]byte, 4096)

		rand.Read(data)

		err = pageLog.Append(1, 1, data)

		if err != nil {
			t.Fatalf("Failed to append data: %v", err)
		}

		err = pageLog.Tombstone(1)

		if err != nil {
			t.Fatalf("Failed to tombstone data: %v", err)
		}

		retrievedData := make([]byte, 4096)

		found, foundVersion, err := pageLog.Get(1, 1, retrievedData)

		if err != nil {
			t.Fatalf("Failed to get data after tombstone: %v", err)
		}

		if found {
			t.Fatal("Expected data to be tombstoned but it was found")
		}

		if foundVersion != 0 {
			t.Fatal("Expected found version to be 0 after tombstone")
		}
	})
}

func TestPageLog_ConcurrentSync(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		pageLog, err := storage.NewPageLog(
			app.Cluster.TieredFS(),
			"PAGE_LOG",
		)

		if err != nil {
			t.Fatalf("Failed to create new page log: %v", err)
		}

		// Add some data
		data := make([]byte, 4096)
		rand.Read(data)

		err = pageLog.Append(1, 1, data)

		if err != nil {
			t.Fatalf("Failed to append data: %v", err)
		}

		// Test concurrent sync operations
		numGoroutines := 10
		errChan := make(chan error, numGoroutines)

		for range numGoroutines {
			go func() {
				errChan <- pageLog.Sync()
			}()
		}

		// Check all sync operations completed successfully
		for range numGoroutines {
			err := <-errChan
			if err != nil {
				t.Fatalf("Concurrent sync failed: %v", err)
			}
		}

		// Verify data is still accessible after concurrent syncs
		readData := make([]byte, 4096)

		found, foundVersion, err := pageLog.Get(1, 1, readData)

		if err != nil {
			t.Fatalf("Failed to read data after concurrent sync: %v", err)
		}

		if !found {
			t.Fatal("Data not found after concurrent sync")
		}

		if foundVersion != 1 {
			t.Fatalf("Expected version 1, got %d", foundVersion)
		}

		if !bytes.Equal(data, readData) {
			t.Fatal("Data mismatch after concurrent sync")
		}
	})
}
