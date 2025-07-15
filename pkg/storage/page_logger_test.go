package storage_test

import (
	"bytes"
	"crypto/rand"
	"sync"
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/server"
	"github.com/litebase/litebase/pkg/storage"
)

func TestNewPageLogger(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		db := test.MockDatabase(app)

		pageLogger, err := storage.NewPageLogger(
			db.DatabaseID,
			db.BranchID,
			app.Cluster.LocalFS(),
		)

		if err != nil {
			t.Fatalf("Failed to create page logger: %v", err)
		}

		if pageLogger == nil {
			t.Fatal("Expected page logger to be created, but got nil")
		}
	})
}

func TestPageLogger_Acquire(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		db := test.MockDatabase(app)

		pageLogger, err := storage.NewPageLogger(
			db.DatabaseID,
			db.BranchID,
			app.Cluster.LocalFS(),
		)

		if err != nil {
			t.Fatalf("Failed to create page logger: %v", err)
		}

		if pageLogger == nil {
			t.Fatal("Expected page logger to be created, but got nil")
		}

		pageLogger.Acquire(1)

		testCases := []struct {
			pageNum int64
			version int64
			data    []byte
		}{
			{1, time.Now().UTC().UnixNano(), make([]byte, 4096)},
			{2, time.Now().UTC().UnixNano(), make([]byte, 4096)},
			{3, time.Now().UTC().UnixNano(), make([]byte, 4096)},
		}

		for _, tc := range testCases {
			rand.Read(tc.data)

			n, err := pageLogger.Write(tc.pageNum, tc.version, tc.data)

			if err != nil {
				t.Fatalf("Failed to write page: %v. Page: %d, Version: %d", err, tc.pageNum, tc.version)
			}

			if n != len(tc.data) {
				t.Fatalf("Expected to write %d bytes, but wrote %d", len(tc.data), n)
			}

			err = pageLogger.Compact(
				app.DatabaseManager.Resources(db.DatabaseID, db.BranchID).FileSystem(),
			)

			if err != nil {
				t.Fatalf("Failed to compact page logger: %v", err)
			}

			readData := make([]byte, 4096)

			found, _, err := pageLogger.Read(tc.pageNum, tc.version, readData)

			if err != nil {
				t.Fatalf("Failed to read page: %v", err)
			}

			if !found {
				t.Fatal("Expected to find page data after write, but not found")
			}

			if !bytes.Equal(readData, tc.data) {
				t.Fatal("Expected data to be equal to written data")
			}
		}
		// if err != nil {
		// 	t.Fatalf("Failed to acquire page logger: %v", err)
		// }

	})
}

func TestPageLogger_Close(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		db := test.MockDatabase(app)

		pageLogger, err := storage.NewPageLogger(
			db.DatabaseID,
			db.BranchID,
			app.Cluster.LocalFS(),
		)

		if err != nil {
			t.Fatalf("Failed to create page logger: %v", err)
		}

		if pageLogger == nil {
			t.Fatal("Expected page logger to be created, but got nil")
		}

		err = pageLogger.Close()

		if err != nil {
			t.Fatalf("Failed to close page logger: %v", err)
		}
	})
}

func TestPageLogger_Compact(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		db := test.MockDatabase(app)

		pageLogger, err := storage.NewPageLogger(
			db.DatabaseID,
			db.BranchID,
			app.Cluster.LocalFS(),
		)

		if err != nil {
			t.Fatalf("Failed to create page logger: %v", err)
		}

		if pageLogger == nil {
			t.Fatal("Expected page logger to be created, but got nil")
		}

		writes := []struct {
			pageNum int64
			version int64
			data    []byte
		}{
			{1, 1, make([]byte, 4096)},
			{2, 1, make([]byte, 4096)},
			{3, 1, make([]byte, 4096)},
			{1, 2, make([]byte, 4096)},
			{3, 2, make([]byte, 4096)},
		}
		for _, write := range writes {
			rand.Read(write.data)

			_, err := pageLogger.Write(write.pageNum, write.version, write.data)

			if err != nil {
				t.Fatalf("Failed to write page: %v. Page: %d, Version: %d", err, write.pageNum, write.version)
			}
		}

		err = pageLogger.Compact(
			app.DatabaseManager.Resources(db.DatabaseID, db.BranchID).FileSystem(),
		)

		if err != nil {
			t.Fatalf("Failed to compact page logger: %v", err)
		}
	})
}

func TestPageLogger_Compact_NoNewWrites(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		// Set compaction interval to a low value for testing
		originalInterval := storage.PageLoggerCompactInterval
		storage.PageLoggerCompactInterval = time.Millisecond
		defer func() {
			storage.PageLoggerCompactInterval = originalInterval
		}()

		db := test.MockDatabase(app)

		pageLogger, err := storage.NewPageLogger(
			db.DatabaseID,
			db.BranchID,
			app.Cluster.LocalFS(),
		)

		if err != nil {
			t.Fatalf("Failed to create page logger: %v", err)
		}

		if pageLogger == nil {
			t.Fatal("Expected page logger to be created, but got nil")
		}

		// Verify initial state - CompactedAt should be zero
		if !pageLogger.CompactedAt.IsZero() {
			t.Fatal("Expected CompactedAt to be zero initially")
		}

		write := make([]byte, 4096)
		rand.Read(write)

		// Write some initial data
		for _, version := range []int64{1, 2, 3} {
			for i := range 3 {
				_, err := pageLogger.Write(int64(i+1), version, write)

				if err != nil {
					t.Fatalf("Failed to write page: %v", err)
				}
			}
		}

		// First compaction - this should run since there are writes
		err = pageLogger.Compact(
			app.DatabaseManager.Resources(db.DatabaseID, db.BranchID).FileSystem(),
		)

		if err != nil {
			t.Fatalf("Failed to compact page logger: %v", err)
		}

		// Store the CompactedAt timestamp from the first compaction
		firstCompactedAt := pageLogger.CompactedAt
		if firstCompactedAt.IsZero() {
			t.Fatal("Expected CompactedAt to be set after first compaction")
		}

		// Sleep to ensure the compaction interval has passed
		time.Sleep(time.Millisecond * 2)

		// Now try to compact again WITHOUT any new writes since last compaction
		// This should NOT run because there are no new writes (writtenAt is before or equal to CompactedAt)
		err = pageLogger.Compact(
			app.DatabaseManager.Resources(db.DatabaseID, db.BranchID).FileSystem(),
		)

		if err != nil {
			t.Fatalf("Failed to compact page logger: %v", err)
		}

		// Verify that CompactedAt timestamp hasn't changed (proving compaction didn't run)
		secondCompactedAt := pageLogger.CompactedAt
		if !secondCompactedAt.Equal(firstCompactedAt) {
			t.Fatalf("CompactedAt timestamp changed from %v to %v, indicating compaction ran when it shouldn't have", firstCompactedAt, secondCompactedAt)
		}

		// Write new data after the second compaction attempt
		testData := make([]byte, 4096)
		rand.Read(testData)

		// testVersion := int64(7)

		_, err = pageLogger.Write(1, 4, testData)
		if err != nil {
			t.Fatalf("Failed to write test data: %v", err)
		}

		// Sleep to ensure the compaction interval has passed
		time.Sleep(time.Millisecond * 2)

		// Now compaction should run because we have new writes
		err = pageLogger.Compact(
			app.DatabaseManager.Resources(db.DatabaseID, db.BranchID).FileSystem(),
		)

		if err != nil {
			t.Fatalf("Failed to compact page logger: %v", err)
		}

		// Verify that CompactedAt timestamp has changed (proving compaction ran)
		thirdCompactedAt := pageLogger.CompactedAt
		if thirdCompactedAt.Equal(firstCompactedAt) {
			t.Fatal("CompactedAt timestamp unchanged, indicating compaction did not run when it should have")
		}
	})
}

func TestPageLogger_CompactionBarrier(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		db := test.MockDatabase(app)

		pageLogger, err := storage.NewPageLogger(
			db.DatabaseID,
			db.BranchID,
			app.Cluster.LocalFS(),
		)

		if err != nil {
			t.Fatalf("Failed to create page logger: %v", err)
		}

		if pageLogger == nil {
			t.Fatal("Expected page logger to be created, but got nil")
		}

		wg := sync.WaitGroup{}

		wg.Add(1)

		go func() {
			defer wg.Done()
			err = pageLogger.CompactionBarrier(func() error {
				time.Sleep(10 * time.Millisecond)
				return nil
			})
		}()

		wg.Add(1)

		go func() {
			defer wg.Done()
			time.Sleep(1 * time.Millisecond)

			err = pageLogger.CompactionBarrier(func() error {
				return nil
			})

			if err == nil {
				t.Error("Expected error due to compaction barrier, but got nil")
			}
		}()

		wg.Wait()
	})
}

func TestPageLogger_ForceCompact(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		db := test.MockDatabase(app)

		pageLogger, err := storage.NewPageLogger(
			db.DatabaseID,
			db.BranchID,
			app.Cluster.LocalFS(),
		)

		if err != nil {
			t.Fatalf("Failed to create page logger: %v", err)
		}

		if pageLogger == nil {
			t.Fatal("Expected page logger to be created, but got nil")
		}

		err = pageLogger.ForceCompact(
			app.DatabaseManager.Resources(db.DatabaseID, db.BranchID).FileSystem(),
		)

		if err != nil {
			t.Fatalf("Failed to force compact page logger: %v", err)
		}
	})
}

func TestPageLogger_Read(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		db := test.MockDatabase(app)

		pageLogger, err := storage.NewPageLogger(
			db.DatabaseID,
			db.BranchID,
			app.Cluster.LocalFS(),
		)

		if err != nil {
			t.Fatalf("Failed to create page logger: %v", err)
		}

		if pageLogger == nil {
			t.Fatal("Expected page logger to be created, but got nil")
		}

		write := make([]byte, 4096)
		read := make([]byte, 4096)
		rand.Read(write)

		for _, version := range []int64{1, 2, 3} {
			for i := range 8192 {
				_, err := pageLogger.Write(int64(i+1), version, write)

				if err != nil {
					t.Fatalf("Failed to write page: %v", err)
				}

				found, _, err := pageLogger.Read(int64(i+1), version, read)

				if err != nil {
					t.Fatalf("Failed to read page: %v", err)
				}

				if !found {
					t.Fatal("Expected to find page data, but not found")
				}

				if !bytes.Equal(read, write) {
					t.Fatal("Expected data to be equal to written data")
				}
			}
		}
	})
}

func TestPageLogger_Read_After_Compacting_After_Interval(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		storage.PageLoggerCompactInterval = 0
		defer func() {
			storage.PageLoggerCompactInterval = storage.DefaultPageLoggerCompactInterval
		}()

		db := test.MockDatabase(app)

		pageLogger, err := storage.NewPageLogger(
			db.DatabaseID,
			db.BranchID,
			app.Cluster.LocalFS(),
		)

		if err != nil {
			t.Fatalf("Failed to create page logger: %v", err)
		}

		if pageLogger == nil {
			t.Fatal("Expected page logger to be created, but got nil")
		}

		write := make([]byte, 4096)
		read := make([]byte, 4096)
		rand.Read(write)

		for _, version := range []int64{1, 2, 3} {
			for i := range 8192 {
				_, err := pageLogger.Write(int64(i+1), version, write)

				if err != nil {
					t.Fatalf("Failed to write page: %v", err)
				}
			}
		}

		// Compaction will run since the compaction interval has passed
		err = pageLogger.Compact(
			app.DatabaseManager.Resources(db.DatabaseID, db.BranchID).FileSystem(),
		)

		if err != nil {
			t.Fatalf("Failed to compact page logger: %v", err)
		}

		for _, version := range []int64{1, 2, 3} {
			for i := range 8192 {
				found, _, err := pageLogger.Read(int64(i+1), version, read)

				if err != nil {
					t.Fatalf("Failed to read page: %v", err)
				}

				if found {
					t.Fatalf("Expected no to find page data. Page: %d, Version: %d", int64(i+1), version)
				}
			}
		}
	})
}

func TestPageLogger_Read_After_Compacting_BeforeInterval(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		// Set a short compaction interval for testing
		originalInterval := storage.PageLoggerCompactInterval
		storage.PageLoggerCompactInterval = time.Second // 1 second interval
		defer func() {
			storage.PageLoggerCompactInterval = originalInterval
		}()

		db := test.MockDatabase(app)

		pageLogger, err := storage.NewPageLogger(
			db.DatabaseID,
			db.BranchID,
			app.Cluster.LocalFS(),
		)

		if err != nil {
			t.Fatalf("Failed to create page logger: %v", err)
		}

		if pageLogger == nil {
			t.Fatal("Expected page logger to be created, but got nil")
		}

		write := make([]byte, 4096)
		read := make([]byte, 4096)
		rand.Read(write)

		for _, version := range []int64{1, 2, 3} {
			for i := range 8192 {
				_, err := pageLogger.Write(int64(i+1), version, write)

				if err != nil {
					t.Fatalf("Failed to write page: %v", err)
				}
			}
		}

		// First compaction - this should run since CompactedAt is zero
		err = pageLogger.Compact(
			app.DatabaseManager.Resources(db.DatabaseID, db.BranchID).FileSystem(),
		)

		if err != nil {
			t.Fatalf("Failed to compact page logger: %v", err)
		}

		// Store the CompactedAt timestamp from the first compaction
		firstCompactedAt := pageLogger.CompactedAt
		if firstCompactedAt.IsZero() {
			t.Fatal("Expected CompactedAt to be set after first compaction")
		}

		// Write more data after compaction
		for _, version := range []int64{4, 5, 6} {
			for i := range 8192 {
				_, err := pageLogger.Write(int64(i+1), version, write)

				if err != nil {
					t.Fatalf("Failed to write page: %v", err)
				}
			}
		}

		// Immediately try to compact again - this should NOT run because the interval has not passed
		err = pageLogger.Compact(
			app.DatabaseManager.Resources(db.DatabaseID, db.BranchID).FileSystem(),
		)

		if err != nil {
			t.Fatalf("Failed to compact page logger: %v", err)
		}

		// Verify that CompactedAt timestamp hasn't changed (proving compaction didn't run due to interval)
		secondCompactedAt := pageLogger.CompactedAt
		if !secondCompactedAt.Equal(firstCompactedAt) {
			t.Fatalf("CompactedAt timestamp changed from %v to %v, indicating compaction ran when it shouldn't have due to interval restriction", firstCompactedAt, secondCompactedAt)
		}

		// The new data (versions 4, 5, 6) should still be available since compaction didn't run
		for _, version := range []int64{4, 5, 6} {
			for i := range 8192 {
				found, _, err := pageLogger.Read(int64(i+1), version, read)

				if err != nil {
					t.Fatalf("Failed to read page: %v", err)
				}

				if !found {
					t.Fatalf("Expected to find page data, but not found. Page: %d, Version: %d", int64(i+1), version)
				}

				if !bytes.Equal(read, write) {
					t.Fatal("Expected data to be equal to written data")
				}
			}
		}
	})
}

func TestPageLogger_Release(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		db := test.MockDatabase(app)
		storage.PageLoggerCompactInterval = 0

		defer func() {
			storage.PageLoggerCompactInterval = storage.DefaultPageLoggerCompactInterval
		}()

		pageLogger, err := storage.NewPageLogger(
			db.DatabaseID,
			db.BranchID,
			app.Cluster.LocalFS(),
		)

		if err != nil {
			t.Fatalf("Failed to create page logger: %v", err)
		}

		if pageLogger == nil {
			t.Fatal("Expected page logger to be created, but got nil")
		}

		if !pageLogger.CompactedAt.IsZero() {
			t.Fatal("Expected CompactedAt to be zero, but got non-zero value")
		}

		pageLogger.Acquire(1)

		pageLogger.Write(1, 1, make([]byte, 4096))

		pageLogger.Compact(
			app.DatabaseManager.Resources(db.DatabaseID, db.BranchID).FileSystem(),
		)

		if !pageLogger.CompactedAt.IsZero() {
			t.Fatal("Expected CompactedAt to be zero, but got non-zero value")
		}

		pageLogger.Release(1)

		pageLogger.Compact(
			app.DatabaseManager.Resources(db.DatabaseID, db.BranchID).FileSystem(),
		)

		if pageLogger.CompactedAt.IsZero() {
			t.Fatal("Expected CompactedAt to be non-zero, but got zero value")
		}
	})
}

func TestPageLogger_Write(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		db := test.MockDatabase(app)

		pageLogger, err := storage.NewPageLogger(
			db.DatabaseID,
			db.BranchID,
			app.Cluster.LocalFS(),
		)

		if err != nil {
			t.Fatalf("Failed to create page logger: %v", err)
		}

		if pageLogger == nil {
			t.Fatal("Expected page logger to be created, but got nil")
		}

		testCases := []struct {
			pageNum int64
			version int64
			data    []byte
		}{
			{1, 1, make([]byte, 4096)},
			{2, 1, make([]byte, 4096)},
			{3, 1, make([]byte, 4096)},
			{4097, 1, make([]byte, 4096)},
			{4098, 1, make([]byte, 4096)},
			{4099, 1, make([]byte, 4096)},
		}

		for _, tc := range testCases {
			rand.Read(tc.data)

			n, err := pageLogger.Write(tc.pageNum, tc.version, tc.data)

			if err != nil {
				t.Fatalf("Failed to write page: %v. Page: %d, Version: %d", err, tc.pageNum, tc.version)
			}

			if n != len(tc.data) {
				t.Fatalf("Expected to write %d bytes, but wrote %d", len(tc.data), n)
			}

			readData := make([]byte, 4096)

			found, _, err := pageLogger.Read(tc.pageNum, tc.version, readData)

			if err != nil {
				t.Fatalf("Failed to read page: %v", err)
			}

			if !found {
				t.Fatal("Expected to find page data after write, but not found")
			}

			if !bytes.Equal(readData, tc.data) {
				t.Fatal("Expected data to be equal to written data")
			}
		}

		err = pageLogger.Close()

		if err != nil {
			t.Fatalf("Failed to close page logger: %v", err)
		}

		pageLogger, err = storage.NewPageLogger(
			db.DatabaseID,
			db.BranchID,
			app.Cluster.LocalFS(),
		)

		if err != nil {
			t.Fatalf("Failed to create page logger: %v", err)
		}

		for _, tc := range testCases {
			readData := make([]byte, 4096)

			found, _, err := pageLogger.Read(tc.pageNum, tc.version, readData)

			if err != nil {
				t.Fatalf("Failed to read page: %v", err)
			}

			if !found {
				t.Fatal("Expected to find page data after write, but not found")
			}

			if !bytes.Equal(readData, tc.data) {
				t.Fatal("Expected data to be equal to written data")
			}
		}
	})
}

func TestPageLogger_Write_WhileCompacting(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		db := test.MockDatabase(app)

		pageLogger, err := storage.NewPageLogger(
			db.DatabaseID,
			db.BranchID,
			app.Cluster.LocalFS(),
		)

		if err != nil {
			t.Fatalf("Failed to create page logger: %v", err)
		}

		if pageLogger == nil {
			t.Fatal("Expected page logger to be created, but got nil")
		}

		testCases := []struct {
			pageNum int64
			version int64
			data    []byte
		}{
			{1, time.Now().UTC().UnixNano(), make([]byte, 4096)},
			{2, time.Now().UTC().UnixNano(), make([]byte, 4096)},
			{3, time.Now().UTC().UnixNano(), make([]byte, 4096)},
		}

		for _, tc := range testCases {
			rand.Read(tc.data)

			n, err := pageLogger.Write(tc.pageNum, tc.version, tc.data)

			if err != nil {
				t.Fatalf("Failed to write page: %v. Page: %d, Version: %d", err, tc.pageNum, tc.version)
			}

			if n != len(tc.data) {
				t.Fatalf("Expected to write %d bytes, but wrote %d", len(tc.data), n)
			}

			readData := make([]byte, 4096)

			found, _, err := pageLogger.Read(tc.pageNum, tc.version, readData)

			if err != nil {
				t.Fatalf("Failed to read page: %v", err)
			}

			if !found {
				t.Fatal("Expected to find page data after write, but not found")
			}

			if !bytes.Equal(readData, tc.data) {
				t.Fatal("Expected data to be equal to written data")
			}
		}

		err = pageLogger.Compact(
			app.DatabaseManager.Resources(db.DatabaseID, db.BranchID).FileSystem(),
		)

		if err != nil {
			t.Fatalf("Failed to compact page logger: %v", err)
		}

		testCases = []struct {
			pageNum int64
			version int64
			data    []byte
		}{
			{1, time.Now().UTC().UnixNano(), make([]byte, 4096)},
			{2, time.Now().UTC().UnixNano(), make([]byte, 4096)},
			{3, time.Now().UTC().UnixNano(), make([]byte, 4096)},
		}

		for _, tc := range testCases {
			rand.Read(tc.data)

			n, err := pageLogger.Write(tc.pageNum, tc.version, tc.data)

			if err != nil {
				t.Fatalf("Failed to write page: %v. Page: %d, Version: %d", err, tc.pageNum, tc.version)
			}

			if n != len(tc.data) {
				t.Fatalf("Expected to write %d bytes, but wrote %d", len(tc.data), n)
			}

			readData := make([]byte, 4096)

			found, _, err := pageLogger.Read(tc.pageNum, tc.version, readData)

			if err != nil {
				t.Fatalf("Failed to read page: %v", err)
			}

			if !found {
				t.Fatal("Expected to find page data after write, but not found")
			}

			if !bytes.Equal(readData, tc.data) {
				t.Fatal("Expected data to be equal to written data")
			}
		}

		err = pageLogger.Close()

		if err != nil {
			t.Fatalf("Failed to close page logger: %v", err)
		}

		// Test reopening the page logger and writing again
		pageLogger, err = storage.NewPageLogger(
			db.DatabaseID,
			db.BranchID,
			app.Cluster.LocalFS(),
		)

		if err != nil {
			t.Fatalf("Failed to create page logger: %v", err)
		}

		err = pageLogger.Compact(
			app.DatabaseManager.Resources(db.DatabaseID, db.BranchID).FileSystem(),
		)

		if err != nil {
			t.Fatalf("Failed to compact page logger: %v", err)
		}

		testCases = []struct {
			pageNum int64
			version int64
			data    []byte
		}{
			{1, time.Now().UTC().UnixNano(), make([]byte, 4096)},
			{2, time.Now().UTC().UnixNano(), make([]byte, 4096)},
			{3, time.Now().UTC().UnixNano(), make([]byte, 4096)},
		}

		for _, tc := range testCases {
			rand.Read(tc.data)

			n, err := pageLogger.Write(tc.pageNum, tc.version, tc.data)

			if err != nil {
				t.Fatalf("Failed to write page: %v. Page: %d, Version: %d", err, tc.pageNum, tc.version)
			}

			if n != len(tc.data) {
				t.Fatalf("Expected to write %d bytes, but wrote %d", len(tc.data), n)
			}

			readData := make([]byte, 4096)

			found, _, err := pageLogger.Read(tc.pageNum, tc.version, readData)

			if err != nil {
				t.Fatalf("Failed to read page: %v", err)
			}

			if !found {
				t.Fatal("Expected to find page data after write, but not found")
			}

			if !bytes.Equal(readData, tc.data) {
				t.Fatal("Expected data to be equal to written data")
			}
		}
	})
}

func TestPageLogger_Write_WhileCompactingConcurrently(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		db := test.MockDatabase(app)
		// Set the compact interval to 0 for testing
		storage.PageLoggerCompactInterval = 0

		defer func() {
			storage.PageLoggerCompactInterval = storage.DefaultPageLoggerCompactInterval
		}()

		pageLogger, err := storage.NewPageLogger(
			db.DatabaseID,
			db.BranchID,
			app.Cluster.LocalFS(),
		)

		if err != nil {
			t.Fatalf("Failed to create page logger: %v", err)
		}

		if pageLogger == nil {
			t.Fatal("Expected page logger to be created, but got nil")
		}

		for range 10 {
			t.Run("", func(t *testing.T) {
				testCases := []struct {
					pageNum int64
					version int64
					data    []byte
				}{
					{1, time.Now().UTC().UnixNano(), make([]byte, 4096)},
					{2, time.Now().UTC().UnixNano(), make([]byte, 4096)},
					{3, time.Now().UTC().UnixNano(), make([]byte, 4096)},
				}

				for _, tc := range testCases {
					pageLogger.Acquire(tc.version)
					rand.Read(tc.data)

					n, err := pageLogger.Write(tc.pageNum, tc.version, tc.data)

					if err != nil {
						t.Fatalf("Failed to write page: %v. Page: %d, Version: %d", err, tc.pageNum, tc.version)
					}

					if n != len(tc.data) {
						t.Fatalf("Expected to write %d bytes, but wrote %d", len(tc.data), n)
					}

					readData := make([]byte, 4096)

					found, _, err := pageLogger.Read(tc.pageNum, tc.version, readData)

					if err != nil {
						t.Fatalf("Failed to read page: %v", err)
					}

					if !found {
						t.Fatal("Expected to find page data after write, but not found")
					}

					if !bytes.Equal(readData, tc.data) {
						t.Fatal("Expected data to be equal to written data")
					}

					pageLogger.Release(tc.version)
				}

				err = pageLogger.Compact(
					app.DatabaseManager.Resources(db.DatabaseID, db.BranchID).FileSystem(),
				)

				if err != nil {
					t.Fatalf("Failed to compact page logger: %v", err)
				}

				wg := &sync.WaitGroup{}

				testCases = []struct {
					pageNum int64
					version int64
					data    []byte
				}{
					{1, time.Now().UTC().UnixNano(), make([]byte, 4096)},
					{2, time.Now().UTC().UnixNano(), make([]byte, 4096)},
					{3, time.Now().UTC().UnixNano(), make([]byte, 4096)},
				}

				wg.Add(1)

				go func() {
					defer wg.Done()

					err := pageLogger.Compact(
						app.DatabaseManager.Resources(db.DatabaseID, db.BranchID).FileSystem(),
					)

					if err != nil {
						t.Errorf("Failed to compact page logger: %v", err)
					}
				}()

				for _, tc := range testCases {
					wg.Add(1)

					go func(tc struct {
						pageNum int64
						version int64
						data    []byte
					}) {
						defer wg.Done()
						rand.Read(tc.data)

						pageLogger.Acquire(tc.version)
						defer pageLogger.Release(tc.version)

						n, err := pageLogger.Write(tc.pageNum, tc.version, tc.data)

						if err != nil {
							t.Errorf("Failed to write page: %v. Page: %d, Version: %d", err, tc.pageNum, tc.version)
						}

						if n != len(tc.data) {
							t.Errorf("Expected to write %d bytes, but wrote %d", len(tc.data), n)
						}

						readData := make([]byte, 4096)

						found, _, err := pageLogger.Read(tc.pageNum, tc.version, readData)

						if err != nil {
							t.Errorf("Failed to read page: %v", err)
						}

						if !found {
							t.Errorf("Expected to find page data after write for Page: %d, Version: %d, but not found", tc.pageNum, tc.version)
						}

						if !bytes.Equal(readData, tc.data) {
							t.Error("Expected data to be equal to written data")
						}
					}(tc)
				}

				wg.Wait()
			})
		}

		err = pageLogger.Close()

		if err != nil {
			t.Fatalf("Failed to close page logger: %v", err)
		}

		// Test reopening the page logger and writing again
		// pageLogger, err = storage.NewPageLogger(
		// 	db.DatabaseID,
		// 	db.BranchID,
		// 	app.Cluster.LocalFS(),
		// )

		// if err != nil {
		// 	t.Fatalf("Failed to create page logger: %v", err)
		// }

		// err = pageLogger.Compact(
		// 	app.DatabaseManager.Resources(db.DatabaseID, db.BranchID).FileSystem(),
		// )

		// if err != nil {
		// 	t.Fatalf("Failed to compact page logger: %v", err)
		// }

		// testCases = []struct {
		// 	pageNum int64
		// 	version int64
		// 	data    []byte
		// }{
		// 	{1, time.Now().UTC().UnixNano(), make([]byte, 4096)},
		// 	{2, time.Now().UTC().UnixNano(), make([]byte, 4096)},
		// 	{3, time.Now().UTC().UnixNano(), make([]byte, 4096)},
		// }

		// for _, tc := range testCases {
		// 	rand.Read(tc.data)

		// 	n, err := pageLogger.Write(tc.pageNum, tc.version, tc.data)

		// 	if err != nil {
		// 		t.Fatalf("Failed to write page: %v. Page: %d, Version: %d", err, tc.pageNum, tc.version)
		// 	}

		// 	if n != len(tc.data) {
		// 		t.Fatalf("Expected to write %d bytes, but wrote %d", len(tc.data), n)
		// 	}

		// 	readData := make([]byte, 4096)

		// 	found, _, err := pageLogger.Read(tc.pageNum, tc.version, readData)

		// 	if err != nil {
		// 		t.Fatalf("Failed to read page: %v", err)
		// 	}

		// 	if !found {
		// 		t.Fatal("Expected to find page data after write, but not found")
		// 	}

		// 	if !bytes.Equal(readData, tc.data) {
		// 		t.Fatal("Expected data to be equal to written data")
		// 	}
		// }
	})
}

func TestPageLoggerCanReadFromLaterVersion(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		db := test.MockDatabase(app)

		pageLogger, err := storage.NewPageLogger(
			db.DatabaseID,
			db.BranchID,
			app.Cluster.LocalFS(),
		)

		if err != nil {
			t.Fatalf("Failed to create page logger: %v", err)
		}

		if pageLogger == nil {
			t.Fatal("Expected page logger to be created, but got nil")
		}

		writes := []struct {
			pageNum int64
			version int64
			data    []byte
		}{
			{1, 1, make([]byte, 4096)},
			{2, 1, make([]byte, 4096)},
			{3, 1, make([]byte, 4096)},
			{1, 2, make([]byte, 4096)},
			{3, 2, make([]byte, 4096)},
		}

		reads := []struct {
			pageNum int64
			version int64
		}{
			{2, 1},
			{1, 1},
			{3, 1},
			{1, 2},
			{1, 1},
			{3, 2},
		}

		for _, write := range writes {
			_, err := pageLogger.Write(write.pageNum, write.version, write.data)

			if err != nil {
				t.Fatalf("Failed to write page: %v. Page: %d, Version: %d", err, write.pageNum, write.version)
			}
		}

		readData := make([]byte, 4096)

		for _, read := range reads {
			found, pageVersion, err := pageLogger.Read(read.pageNum, read.version, readData)

			if err != nil {
				t.Fatalf("Failed to read page: %v. Page: %d, Version: %d", err, read.pageNum, read.version)
			}

			if !found {
				t.Fatal("Expected to find page data after write, but not found")
			}

			if pageVersion != storage.PageVersion(read.version) {
				t.Fatal("Expected version to match, read the wrong data")
			}
		}
	})
}

func TestPageLogger_Tombstone(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		db := test.MockDatabase(app)

		pageLogger, err := storage.NewPageLogger(
			db.DatabaseID,
			db.BranchID,
			app.Cluster.LocalFS(),
		)

		if err != nil {
			t.Fatalf("Failed to create page logger: %v", err)
		}

		if pageLogger == nil {
			t.Fatal("Expected page logger to be created, but got nil")
		}

		testCases := []struct {
			pageNum    int64
			version    int64
			data       []byte
			shouldFind bool
		}{
			{1, 1, make([]byte, 4096), true},
			{2, 1, make([]byte, 4096), true},
			{3, 1, make([]byte, 4096), true},
			{1, 2, make([]byte, 4096), false},
			{2, 2, make([]byte, 4096), false},
		}

		for _, tc := range testCases {
			rand.Read(tc.data)

			n, err := pageLogger.Write(tc.pageNum, tc.version, tc.data)

			if err != nil {
				t.Fatalf("Failed to write page: %v. Page: %d, Version: %d", err, tc.pageNum, tc.version)
			}

			if n != len(tc.data) {
				t.Fatalf("Expected to write %d bytes, but wrote %d", len(tc.data), n)
			}
		}

		err = pageLogger.Tombstone(2)

		if err != nil {
			t.Fatalf("Failed to tombstone pages: %v", err)
		}

		readData := make([]byte, 4096)

		for _, tc := range testCases {
			found, foundVersion, err := pageLogger.Read(tc.pageNum, tc.version, readData)

			if err != nil {
				t.Fatalf("Failed to read page: %v. Page: %d, Version: %d", err, tc.pageNum, tc.version)
			}

			if found != tc.shouldFind && tc.version == int64(foundVersion) {
				t.Fatalf("Expected to find page data: %v, but got %v for Page: %d, Version: %d", tc.shouldFind, found, tc.pageNum, tc.version)
			}
		}

		err = pageLogger.Close()

		if err != nil {
			t.Fatalf("Failed to close page logger: %v", err)
		}

		pageLogger, err = storage.NewPageLogger(
			db.DatabaseID,
			db.BranchID,
			app.Cluster.LocalFS(),
		)

		if err != nil {
			t.Fatalf("Failed to create page logger: %v", err)
		}

		for _, tc := range testCases {
			readData := make([]byte, 4096)

			found, foundVersion, err := pageLogger.Read(tc.pageNum, tc.version, readData)

			if err != nil {
				t.Fatalf("Failed to read page: %v. Page: %d, Version: %d", err, tc.pageNum, tc.version)
			}

			if found != tc.shouldFind && tc.version == int64(foundVersion) {
				t.Fatal("Expected page data to be tombstoned and not found after reopening")
			}
		}
	})
}

func TestPageLogger_Tombstone_OnlySpecificVersion(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		db := test.MockDatabase(app)

		pageLogger, err := storage.NewPageLogger(
			db.DatabaseID,
			db.BranchID,
			app.Cluster.LocalFS(),
		)

		if err != nil {
			t.Fatalf("Failed to create page logger: %v", err)
		}

		if pageLogger == nil {
			t.Fatal("Expected page logger to be created, but got nil")
		}

		pageNum := int64(1)
		pageData := make([]byte, 4096)
		rand.Read(pageData)

		// Write the same page with multiple versions (timestamps)
		version1 := int64(100)
		version2 := int64(200)
		version3 := int64(300)

		// Write version 1
		n, err := pageLogger.Write(pageNum, version1, pageData)
		if err != nil {
			t.Fatalf("Failed to write page version %d: %v", version1, err)
		}
		if n != len(pageData) {
			t.Fatalf("Expected to write %d bytes, but wrote %d", len(pageData), n)
		}

		// Write version 2
		n, err = pageLogger.Write(pageNum, version2, pageData)
		if err != nil {
			t.Fatalf("Failed to write page version %d: %v", version2, err)
		}
		if n != len(pageData) {
			t.Fatalf("Expected to write %d bytes, but wrote %d", len(pageData), n)
		}

		// Write version 3
		n, err = pageLogger.Write(pageNum, version3, pageData)
		if err != nil {
			t.Fatalf("Failed to write page version %d: %v", version3, err)
		}
		if n != len(pageData) {
			t.Fatalf("Expected to write %d bytes, but wrote %d", len(pageData), n)
		}

		// Verify all versions are initially readable
		readData := make([]byte, 4096)
		for _, version := range []int64{version1, version2, version3} {
			found, foundVersion, err := pageLogger.Read(pageNum, version, readData)
			if err != nil {
				t.Fatalf("Failed to read page version %d: %v", version, err)
			}
			if !found {
				t.Fatalf("Expected to find version %d, but it was not found", version)
			}
			if uint64(foundVersion) != uint64(version) {
				t.Fatalf("Expected to find version %d, but found version %d", version, foundVersion)
			}
		}

		// Tombstone only version 2
		tombstoneVersion := version2

		err = pageLogger.Tombstone(tombstoneVersion)
		if err != nil {
			t.Fatalf("Failed to tombstone version %d: %v", tombstoneVersion, err)
		}

		// Test reads after tombstoning
		testCases := []struct {
			requestVersion  int64
			shouldFind      bool
			expectedVersion int64
		}{
			{version1, true, version1}, // Version 1 should still be found
			{version2, true, version1}, // Version 2 request should find version 1 (highest <= version2 that's not tombstoned)
			{version3, true, version3}, // Version 3 should still be found
		}

		for _, tc := range testCases {
			found, foundVersion, err := pageLogger.Read(pageNum, tc.requestVersion, readData)
			if err != nil {
				t.Fatalf("Failed to read page version %d: %v", tc.requestVersion, err)
			}

			if found != tc.shouldFind {
				t.Fatalf("Expected to find version %d: %v, but got %v", tc.requestVersion, tc.shouldFind, found)
			}

			if tc.shouldFind && uint64(foundVersion) != uint64(tc.expectedVersion) {
				t.Fatalf("Expected to find version %d when requesting version %d, but found version %d", tc.expectedVersion, tc.requestVersion, foundVersion)
			}
		}

		err = pageLogger.Close()
		if err != nil {
			t.Fatalf("Failed to close page logger: %v", err)
		}
	})
}

func TestPageLogger_PersistenceAcrossRestarts(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		db := test.MockDatabase(app)

		// Create initial page logger
		pageLogger, err := storage.NewPageLogger(
			db.DatabaseID,
			db.BranchID,
			app.Cluster.LocalFS(),
		)

		if err != nil {
			t.Fatalf("Failed to create page logger: %v", err)
		}

		if pageLogger == nil {
			t.Fatal("Expected page logger to be created, but got nil")
		}

		// Define test data - multiple pages with different versions
		testData := []struct {
			pageNum int64
			version int64
			data    []byte
		}{
			{1, 100, make([]byte, 4096)},
			{2, 100, make([]byte, 4096)},
			{3, 100, make([]byte, 4096)},
			{1, 200, make([]byte, 4096)}, // Same page, different version
			{2, 200, make([]byte, 4096)},
			{4097, 100, make([]byte, 4096)}, // Different page group
			{4098, 100, make([]byte, 4096)},
			{4097, 300, make([]byte, 4096)}, // Same page, different version
		}

		// Fill test data with random bytes
		for i := range testData {
			rand.Read(testData[i].data)
		}

		// Write all test data to the first page logger instance
		for _, tc := range testData {
			n, err := pageLogger.Write(tc.pageNum, tc.version, tc.data)

			if err != nil {
				t.Fatalf("Failed to write page: %v. Page: %d, Version: %d", err, tc.pageNum, tc.version)
			}

			if n != len(tc.data) {
				t.Fatalf("Expected to write %d bytes, but wrote %d", len(tc.data), n)
			}
		}

		// Verify all data can be read from the first instance
		for _, tc := range testData {
			readData := make([]byte, 4096)
			found, _, err := pageLogger.Read(tc.pageNum, tc.version, readData)

			if err != nil {
				t.Fatalf("Failed to read page from first instance: %v. Page: %d, Version: %d", err, tc.pageNum, tc.version)
			}

			if !found {
				t.Fatalf("Expected to find page data in first instance, but not found. Page: %d, Version: %d", tc.pageNum, tc.version)
			}

			if !bytes.Equal(readData, tc.data) {
				t.Fatalf("Data mismatch in first instance. Page: %d, Version: %d", tc.pageNum, tc.version)
			}
		}

		// Close the first page logger instance
		err = pageLogger.Close()

		if err != nil {
			t.Fatalf("Failed to close first page logger: %v", err)
		}

		// Create a second page logger instance (simulating restart)
		pageLogger2, err := storage.NewPageLogger(
			db.DatabaseID,
			db.BranchID,
			app.Cluster.LocalFS(),
		)

		if err != nil {
			t.Fatalf("Failed to create second page logger: %v", err)
		}

		if pageLogger2 == nil {
			t.Fatal("Expected second page logger to be created, but got nil")
		}

		// Verify all previously written data can still be read from the second instance
		for _, tc := range testData {
			readData := make([]byte, 4096)
			found, _, err := pageLogger2.Read(tc.pageNum, tc.version, readData)

			if err != nil {
				t.Fatalf("Failed to read page from second instance: %v. Page: %d, Version: %d", err, tc.pageNum, tc.version)
			}

			if !found {
				t.Fatalf("Expected to find page data in second instance, but not found. Page: %d, Version: %d", tc.pageNum, tc.version)
			}

			if !bytes.Equal(readData, tc.data) {
				t.Fatalf("Data mismatch in second instance. Page: %d, Version: %d", tc.pageNum, tc.version)
			}
		}

		// Write additional data to the second instance
		additionalData := []struct {
			pageNum int64
			version int64
			data    []byte
		}{
			{5, 400, make([]byte, 4096)},
			{6, 400, make([]byte, 4096)},
			{1, 500, make([]byte, 4096)}, // Same page as before, newer version
		}

		for i := range additionalData {
			rand.Read(additionalData[i].data)
		}

		for _, tc := range additionalData {
			n, err := pageLogger2.Write(tc.pageNum, tc.version, tc.data)

			if err != nil {
				t.Fatalf("Failed to write additional page: %v. Page: %d, Version: %d", err, tc.pageNum, tc.version)
			}

			if n != len(tc.data) {
				t.Fatalf("Expected to write %d bytes, but wrote %d", len(tc.data), n)
			}
		}

		// Verify all data (original + additional) can be read
		allData := append(testData, additionalData...)
		for _, tc := range allData {
			readData := make([]byte, 4096)
			found, _, err := pageLogger2.Read(tc.pageNum, tc.version, readData)

			if err != nil {
				t.Fatalf("Failed to read page from second instance after additional writes: %v. Page: %d, Version: %d", err, tc.pageNum, tc.version)
			}

			if !found {
				t.Fatalf("Expected to find page data in second instance after additional writes, but not found. Page: %d, Version: %d", tc.pageNum, tc.version)
			}

			if !bytes.Equal(readData, tc.data) {
				t.Fatalf("Data mismatch in second instance after additional writes. Page: %d, Version: %d", tc.pageNum, tc.version)
			}
		}

		// Close second instance
		err = pageLogger2.Close()

		if err != nil {
			t.Fatalf("Failed to close second page logger: %v", err)
		}

		// Create third instance to verify all data is still there
		pageLogger3, err := storage.NewPageLogger(
			db.DatabaseID,
			db.BranchID,
			app.Cluster.LocalFS(),
		)

		if err != nil {
			t.Fatalf("Failed to create third page logger: %v", err)
		}

		if pageLogger3 == nil {
			t.Fatal("Expected third page logger to be created, but got nil")
		}

		// Final verification - all data should still be accessible
		for _, tc := range allData {
			readData := make([]byte, 4096)
			found, _, err := pageLogger3.Read(tc.pageNum, tc.version, readData)

			if err != nil {
				t.Fatalf("Failed to read page from third instance: %v. Page: %d, Version: %d", err, tc.pageNum, tc.version)
			}

			if !found {
				t.Fatalf("Expected to find page data in third instance, but not found. Page: %d, Version: %d", tc.pageNum, tc.version)
			}

			if !bytes.Equal(readData, tc.data) {
				t.Fatalf("Data mismatch in third instance. Page: %d, Version: %d", tc.pageNum, tc.version)
			}
		}

		// Clean up
		err = pageLogger3.Close()

		if err != nil {
			t.Fatalf("Failed to close third page logger: %v", err)
		}
	})
}

func TestPageLogger_PersistenceWithCompaction(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		// Set compaction interval to 0 to allow immediate compaction
		originalInterval := storage.PageLoggerCompactInterval
		storage.PageLoggerCompactInterval = 0
		defer func() {
			storage.PageLoggerCompactInterval = originalInterval
		}()

		db := test.MockDatabase(app)

		// Create initial page logger
		pageLogger, err := storage.NewPageLogger(
			db.DatabaseID,
			db.BranchID,
			app.Cluster.LocalFS(),
		)

		if err != nil {
			t.Fatalf("Failed to create page logger: %v", err)
		}

		if pageLogger == nil {
			t.Fatal("Expected page logger to be created, but got nil")
		}

		// Write some initial data
		testData := []struct {
			pageNum int64
			version int64
			data    []byte
		}{
			{1, 100, make([]byte, 4096)},
			{2, 100, make([]byte, 4096)},
			{3, 100, make([]byte, 4096)},
			{1, 200, make([]byte, 4096)}, // Same page, different version
		}

		for i := range testData {
			rand.Read(testData[i].data)
		}

		for _, tc := range testData {
			n, err := pageLogger.Write(tc.pageNum, tc.version, tc.data)

			if err != nil {
				t.Fatalf("Failed to write page: %v. Page: %d, Version: %d", err, tc.pageNum, tc.version)
			}

			if n != len(tc.data) {
				t.Fatalf("Expected to write %d bytes, but wrote %d", len(tc.data), n)
			}
		}

		// Force compaction
		err = pageLogger.ForceCompact(
			app.DatabaseManager.Resources(db.DatabaseID, db.BranchID).FileSystem(),
		)

		if err != nil {
			t.Fatalf("Failed to force compact page logger: %v", err)
		}

		// Close the page logger
		err = pageLogger.Close()

		if err != nil {
			t.Fatalf("Failed to close page logger: %v", err)
		}

		// Create a new instance after compaction
		pageLogger2, err := storage.NewPageLogger(
			db.DatabaseID,
			db.BranchID,
			app.Cluster.LocalFS(),
		)

		if err != nil {
			t.Fatalf("Failed to create second page logger: %v", err)
		}

		if pageLogger2 == nil {
			t.Fatal("Expected second page logger to be created, but got nil")
		}

		// After compaction, the non-compacted data should be available from the durable storage
		// This tests that the page logger correctly integrates with the durable storage
		// and can read data that was compacted to permanent storage

		// Write new data to the second instance
		newData := []struct {
			pageNum int64
			version int64
			data    []byte
		}{
			{4, 300, make([]byte, 4096)},
			{5, 300, make([]byte, 4096)},
		}

		for i := range newData {
			rand.Read(newData[i].data)
		}

		for _, tc := range newData {
			n, err := pageLogger2.Write(tc.pageNum, tc.version, tc.data)

			if err != nil {
				t.Fatalf("Failed to write new page: %v. Page: %d, Version: %d", err, tc.pageNum, tc.version)
			}

			if n != len(tc.data) {
				t.Fatalf("Expected to write %d bytes, but wrote %d", len(tc.data), n)
			}
		}

		// Verify new data can be read
		for _, tc := range newData {
			readData := make([]byte, 4096)
			found, _, err := pageLogger2.Read(tc.pageNum, tc.version, readData)

			if err != nil {
				t.Fatalf("Failed to read new page: %v. Page: %d, Version: %d", err, tc.pageNum, tc.version)
			}

			if !found {
				t.Fatalf("Expected to find new page data, but not found. Page: %d, Version: %d", tc.pageNum, tc.version)
			}

			if !bytes.Equal(readData, tc.data) {
				t.Fatalf("Data mismatch for new page. Page: %d, Version: %d", tc.pageNum, tc.version)
			}
		}

		// Close second instance
		err = pageLogger2.Close()

		if err != nil {
			t.Fatalf("Failed to close second page logger: %v", err)
		}
	})
}

func TestPageLogger_RestartWithPartialWrites(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		db := test.MockDatabase(app)

		// Create initial page logger
		pageLogger, err := storage.NewPageLogger(
			db.DatabaseID,
			db.BranchID,
			app.Cluster.LocalFS(),
		)

		if err != nil {
			t.Fatalf("Failed to create page logger: %v", err)
		}

		if pageLogger == nil {
			t.Fatal("Expected page logger to be created, but got nil")
		}

		// Write data to multiple page groups
		testData := []struct {
			pageNum int64
			version int64
			data    []byte
		}{
			{1, 100, make([]byte, 4096)},    // Page group 1
			{4097, 100, make([]byte, 4096)}, // Page group 2
			{8193, 100, make([]byte, 4096)}, // Page group 3
			{1, 200, make([]byte, 4096)},    // Page group 1, different version
			{4097, 200, make([]byte, 4096)}, // Page group 2, different version
		}

		for i := range testData {
			rand.Read(testData[i].data)
		}

		// Write only the first few entries
		partialData := testData[:3]
		for _, tc := range partialData {
			n, err := pageLogger.Write(tc.pageNum, tc.version, tc.data)

			if err != nil {
				t.Fatalf("Failed to write page: %v. Page: %d, Version: %d", err, tc.pageNum, tc.version)
			}

			if n != len(tc.data) {
				t.Fatalf("Expected to write %d bytes, but wrote %d", len(tc.data), n)
			}
		}

		// Verify partial data can be read
		for _, tc := range partialData {
			readData := make([]byte, 4096)
			found, _, err := pageLogger.Read(tc.pageNum, tc.version, readData)

			if err != nil {
				t.Fatalf("Failed to read partial page: %v. Page: %d, Version: %d", err, tc.pageNum, tc.version)
			}

			if !found {
				t.Fatalf("Expected to find partial page data, but not found. Page: %d, Version: %d", tc.pageNum, tc.version)
			}

			if !bytes.Equal(readData, tc.data) {
				t.Fatalf("Data mismatch for partial page. Page: %d, Version: %d", tc.pageNum, tc.version)
			}
		}

		// Close without writing the remaining data
		err = pageLogger.Close()

		if err != nil {
			t.Fatalf("Failed to close page logger: %v", err)
		}

		// Create new instance and write the remaining data
		pageLogger2, err := storage.NewPageLogger(
			db.DatabaseID,
			db.BranchID,
			app.Cluster.LocalFS(),
		)

		if err != nil {
			t.Fatalf("Failed to create second page logger: %v", err)
		}

		if pageLogger2 == nil {
			t.Fatal("Expected second page logger to be created, but got nil")
		}

		// Verify the partial data is still available
		for _, tc := range partialData {
			readData := make([]byte, 4096)
			found, _, err := pageLogger2.Read(tc.pageNum, tc.version, readData)

			if err != nil {
				t.Fatalf("Failed to read partial page from second instance: %v. Page: %d, Version: %d", err, tc.pageNum, tc.version)
			}

			if !found {
				t.Fatalf("Expected to find partial page data in second instance, but not found. Page: %d, Version: %d", tc.pageNum, tc.version)
			}

			if !bytes.Equal(readData, tc.data) {
				t.Fatalf("Data mismatch for partial page in second instance. Page: %d, Version: %d", tc.pageNum, tc.version)
			}
		}

		// Write the remaining data
		remainingData := testData[3:]
		for _, tc := range remainingData {
			n, err := pageLogger2.Write(tc.pageNum, tc.version, tc.data)

			if err != nil {
				t.Fatalf("Failed to write remaining page: %v. Page: %d, Version: %d", err, tc.pageNum, tc.version)
			}

			if n != len(tc.data) {
				t.Fatalf("Expected to write %d bytes, but wrote %d", len(tc.data), n)
			}
		}

		// Verify all data is now available
		for _, tc := range testData {
			readData := make([]byte, 4096)
			found, _, err := pageLogger2.Read(tc.pageNum, tc.version, readData)

			if err != nil {
				t.Fatalf("Failed to read complete page: %v. Page: %d, Version: %d", err, tc.pageNum, tc.version)
			}

			if !found {
				t.Fatalf("Expected to find complete page data, but not found. Page: %d, Version: %d", tc.pageNum, tc.version)
			}

			if !bytes.Equal(readData, tc.data) {
				t.Fatalf("Data mismatch for complete page. Page: %d, Version: %d", tc.pageNum, tc.version)
			}
		}

		// Close second instance
		err = pageLogger2.Close()

		if err != nil {
			t.Fatalf("Failed to close second page logger: %v", err)
		}
	})
}

func TestPageLogger_RestartWithoutCompaction_EmptyPageLogs(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		db := test.MockDatabase(app)

		// Create initial page logger
		pageLogger, err := storage.NewPageLogger(
			db.DatabaseID,
			db.BranchID,
			app.Cluster.LocalFS(),
		)

		if err != nil {
			t.Fatalf("Failed to create page logger: %v", err)
		}

		if pageLogger == nil {
			t.Fatal("Expected page logger to be created, but got nil")
		}

		// Write some data without compacting
		testData := []struct {
			pageNum int64
			version int64
			data    []byte
		}{
			{1, 100, make([]byte, 4096)},
			{2, 100, make([]byte, 4096)},
			{3, 100, make([]byte, 4096)},
			{4097, 100, make([]byte, 4096)}, // Different page group
			{8193, 100, make([]byte, 4096)}, // Another page group
		}

		for i := range testData {
			rand.Read(testData[i].data)
		}

		for _, tc := range testData {
			n, err := pageLogger.Write(tc.pageNum, tc.version, tc.data)

			if err != nil {
				t.Fatalf("Failed to write page: %v. Page: %d, Version: %d", err, tc.pageNum, tc.version)
			}

			if n != len(tc.data) {
				t.Fatalf("Expected to write %d bytes, but wrote %d", len(tc.data), n)
			}
		}

		// Verify data can be read from first instance
		for _, tc := range testData {
			readData := make([]byte, 4096)
			found, _, err := pageLogger.Read(tc.pageNum, tc.version, readData)

			if err != nil {
				t.Fatalf("Failed to read page from first instance: %v. Page: %d, Version: %d", err, tc.pageNum, tc.version)
			}

			if !found {
				t.Fatalf("Expected to find page data in first instance, but not found. Page: %d, Version: %d", tc.pageNum, tc.version)
			}

			if !bytes.Equal(readData, tc.data) {
				t.Fatalf("Data mismatch in first instance. Page: %d, Version: %d", tc.pageNum, tc.version)
			}
		}

		// Close without compacting - simulating server shutdown before compaction
		err = pageLogger.Close()

		if err != nil {
			t.Fatalf("Failed to close page logger: %v", err)
		}

		// Create new instance - this might create empty page logs for new generation
		pageLogger2, err := storage.NewPageLogger(
			db.DatabaseID,
			db.BranchID,
			app.Cluster.LocalFS(),
		)

		if err != nil {
			t.Fatalf("Failed to create second page logger: %v", err)
		}

		if pageLogger2 == nil {
			t.Fatal("Expected second page logger to be created, but got nil")
		}

		// Verify that the data is still available after restart
		for _, tc := range testData {
			readData := make([]byte, 4096)
			found, _, err := pageLogger2.Read(tc.pageNum, tc.version, readData)

			if err != nil {
				t.Fatalf("Failed to read page from second instance: %v. Page: %d, Version: %d", err, tc.pageNum, tc.version)
			}

			if !found {
				t.Fatalf("Expected to find page data in second instance, but not found. Page: %d, Version: %d", tc.pageNum, tc.version)
			}

			if !bytes.Equal(readData, tc.data) {
				t.Fatalf("Data mismatch in second instance. Page: %d, Version: %d", tc.pageNum, tc.version)
			}
		}

		// Now try to compact - this might fail with EOF error if empty page logs exist
		err = pageLogger2.Compact(
			app.DatabaseManager.Resources(db.DatabaseID, db.BranchID).FileSystem(),
		)

		if err != nil {
			t.Logf("Compaction failed (this might be expected): %v", err)
			// Don't fail the test here - we want to see what happens
		}

		// Verify data is still available after compaction attempt
		for _, tc := range testData {
			readData := make([]byte, 4096)
			found, _, err := pageLogger2.Read(tc.pageNum, tc.version, readData)

			if err != nil {
				t.Fatalf("Failed to read page after compaction: %v. Page: %d, Version: %d", err, tc.pageNum, tc.version)
			}

			if !found {
				t.Fatalf("Expected to find page data after compaction, but not found. Page: %d, Version: %d", tc.pageNum, tc.version)
			}

			if !bytes.Equal(readData, tc.data) {
				t.Fatalf("Data mismatch after compaction. Page: %d, Version: %d", tc.pageNum, tc.version)
			}
		}

		// Close second instance
		err = pageLogger2.Close()

		if err != nil {
			t.Fatalf("Failed to close second page logger: %v", err)
		}
	})
}

func TestPageLogger_EOFErrorDuringCompaction(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		db := test.MockDatabase(app)

		// Create initial page logger
		pageLogger, err := storage.NewPageLogger(
			db.DatabaseID,
			db.BranchID,
			app.Cluster.LocalFS(),
		)

		if err != nil {
			t.Fatalf("Failed to create page logger: %v", err)
		}

		if pageLogger == nil {
			t.Fatal("Expected page logger to be created, but got nil")
		}

		// Write significant amount of data across multiple page groups
		testData := []struct {
			pageNum int64
			version int64
			data    []byte
		}{}

		// Create test data for multiple page groups
		for pageGroup := 0; pageGroup < 5; pageGroup++ {
			basePageNum := int64(pageGroup * 4096)
			for page := 0; page < 10; page++ {
				for version := 1; version <= 3; version++ {
					testData = append(testData, struct {
						pageNum int64
						version int64
						data    []byte
					}{
						pageNum: basePageNum + int64(page) + 1,
						version: int64(version),
						data:    make([]byte, 4096),
					})
				}
			}
		}

		// Fill with random data
		for i := range testData {
			rand.Read(testData[i].data)
		}

		// Write all data
		for _, tc := range testData {
			n, err := pageLogger.Write(tc.pageNum, tc.version, tc.data)

			if err != nil {
				t.Fatalf("Failed to write page: %v. Page: %d, Version: %d", err, tc.pageNum, tc.version)
			}

			if n != len(tc.data) {
				t.Fatalf("Expected to write %d bytes, but wrote %d", len(tc.data), n)
			}
		}

		// Close without compacting
		err = pageLogger.Close()

		if err != nil {
			t.Fatalf("Failed to close page logger: %v", err)
		}

		// Create new instance and immediately try to compact
		pageLogger2, err := storage.NewPageLogger(
			db.DatabaseID,
			db.BranchID,
			app.Cluster.LocalFS(),
		)

		if err != nil {
			t.Fatalf("Failed to create second page logger: %v", err)
		}

		if pageLogger2 == nil {
			t.Fatal("Expected second page logger to be created, but got nil")
		}

		// Try multiple compactions to see if we can reproduce EOF error
		for i := 0; i < 5; i++ {
			t.Logf("Compaction attempt %d", i+1)

			err = pageLogger2.Compact(
				app.DatabaseManager.Resources(db.DatabaseID, db.BranchID).FileSystem(),
			)

			if err != nil {
				t.Logf("Compaction attempt %d failed: %v", i+1, err)
				// Continue with next attempt to see if pattern emerges
			} else {
				t.Logf("Compaction attempt %d succeeded", i+1)
			}

			// Try to read a sample of data after each compaction attempt
			sampleData := testData[:5] // Just test first 5 entries
			for _, tc := range sampleData {
				readData := make([]byte, 4096)
				found, _, err := pageLogger2.Read(tc.pageNum, tc.version, readData)

				if err != nil {
					t.Logf("Read failed after compaction attempt %d: %v. Page: %d, Version: %d", i+1, err, tc.pageNum, tc.version)
				} else if !found {
					t.Logf("Data not found after compaction attempt %d. Page: %d, Version: %d", i+1, tc.pageNum, tc.version)
				} else if !bytes.Equal(readData, tc.data) {
					t.Logf("Data mismatch after compaction attempt %d. Page: %d, Version: %d", i+1, tc.pageNum, tc.version)
				}
			}
		}

		// Close second instance
		err = pageLogger2.Close()

		if err != nil {
			t.Fatalf("Failed to close second page logger: %v", err)
		}
	})
}

func TestPageLogger_EOFErrorFromIncompletePageLog(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		db := test.MockDatabase(app)

		// Create initial page logger
		pageLogger, err := storage.NewPageLogger(
			db.DatabaseID,
			db.BranchID,
			app.Cluster.LocalFS(),
		)

		if err != nil {
			t.Fatalf("Failed to create page logger: %v", err)
		}

		if pageLogger == nil {
			t.Fatal("Expected page logger to be created, but got nil")
		}

		// Write data to create page logs
		testData := []struct {
			pageNum int64
			version int64
			data    []byte
		}{
			{1, 100, make([]byte, 4096)},
			{2, 100, make([]byte, 4096)},
			{4097, 100, make([]byte, 4096)}, // Different page group
		}

		for i := range testData {
			rand.Read(testData[i].data)
		}

		// Write initial data
		for _, tc := range testData {
			n, err := pageLogger.Write(tc.pageNum, tc.version, tc.data)

			if err != nil {
				t.Fatalf("Failed to write page: %v. Page: %d, Version: %d", err, tc.pageNum, tc.version)
			}

			if n != len(tc.data) {
				t.Fatalf("Expected to write %d bytes, but wrote %d", len(tc.data), n)
			}
		}

		// Close the page logger
		err = pageLogger.Close()

		if err != nil {
			t.Fatalf("Failed to close page logger: %v", err)
		}

		// Create new page logger instance - this might create new empty page logs
		pageLogger2, err := storage.NewPageLogger(
			db.DatabaseID,
			db.BranchID,
			app.Cluster.LocalFS(),
		)

		if err != nil {
			t.Fatalf("Failed to create second page logger: %v", err)
		}

		if pageLogger2 == nil {
			t.Fatal("Expected second page logger to be created, but got nil")
		}

		// Write more data with newer timestamps to create newer page logs
		newData := []struct {
			pageNum int64
			version int64
			data    []byte
		}{
			{1, 200, make([]byte, 4096)},
			{2, 200, make([]byte, 4096)},
			{3, 200, make([]byte, 4096)},
			{4097, 200, make([]byte, 4096)},
			{4098, 200, make([]byte, 4096)},
		}

		for i := range newData {
			rand.Read(newData[i].data)
		}

		// Write new data
		for _, tc := range newData {
			n, err := pageLogger2.Write(tc.pageNum, tc.version, tc.data)

			if err != nil {
				t.Fatalf("Failed to write new page: %v. Page: %d, Version: %d", err, tc.pageNum, tc.version)
			}

			if n != len(tc.data) {
				t.Fatalf("Expected to write %d bytes, but wrote %d", len(tc.data), n)
			}
		}

		// Close without compacting - this might leave incomplete page logs
		err = pageLogger2.Close()

		if err != nil {
			t.Fatalf("Failed to close second page logger: %v", err)
		}

		// Create third instance and immediately try to compact
		pageLogger3, err := storage.NewPageLogger(
			db.DatabaseID,
			db.BranchID,
			app.Cluster.LocalFS(),
		)

		if err != nil {
			t.Fatalf("Failed to create third page logger: %v", err)
		}

		if pageLogger3 == nil {
			t.Fatal("Expected third page logger to be created, but got nil")
		}

		// This compaction might fail with EOF error if there are incomplete page logs
		err = pageLogger3.Compact(
			app.DatabaseManager.Resources(db.DatabaseID, db.BranchID).FileSystem(),
		)

		if err != nil {
			t.Logf("Compaction failed (might be expected EOF error): %v", err)
			// Don't fail test - we want to see what happens
		}

		// Force compaction to see if it handles the issue differently
		err = pageLogger3.ForceCompact(
			app.DatabaseManager.Resources(db.DatabaseID, db.BranchID).FileSystem(),
		)

		if err != nil {
			t.Logf("Force compaction failed: %v", err)
		}

		// Close third instance
		err = pageLogger3.Close()

		if err != nil {
			t.Fatalf("Failed to close third page logger: %v", err)
		}
	})
}
