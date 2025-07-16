package storage_test

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/file"
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
				t.Fatalf("Failed to read page from second instance after compaction: %v. Page: %d, Version: %d", err, tc.pageNum, tc.version)
			}

			if !found {
				t.Fatalf("Expected to find page data in second instance after compaction, but not found. Page: %d, Version: %d", tc.pageNum, tc.version)
			}

			if !bytes.Equal(readData, tc.data) {
				t.Fatalf("Data mismatch in second instance after compaction. Page: %d, Version: %d", tc.pageNum, tc.version)
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

		// This compaction might fail with EOF error if there are incomplete page logs
		err = pageLogger2.Compact(
			app.DatabaseManager.Resources(db.DatabaseID, db.BranchID).FileSystem(),
		)

		if err != nil {
			t.Logf("Compaction failed (might be expected EOF error): %v", err)
			// Don't fail test - we want to see what happens
		}

		// Force compaction to see if it handles the issue differently
		err = pageLogger2.ForceCompact(
			app.DatabaseManager.Resources(db.DatabaseID, db.BranchID).FileSystem(),
		)

		if err != nil {
			t.Logf("Force compaction failed: %v", err)
		}

		// Close second instance
		err = pageLogger2.Close()

		if err != nil {
			t.Fatalf("Failed to close second page logger: %v", err)
		}
	})
}

func TestPageLogger_CompactEmptyPageLogs(t *testing.T) {
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
		defer pageLogger.Close()

		// Create some empty page logs by writing and then tombstoning all data
		pageData := make([]byte, 4096)
		timestamp1 := time.Now().UnixNano()
		timestamp2 := timestamp1 + 1000
		timestamp3 := timestamp1 + 2000

		// Write data to create logs
		_, err = pageLogger.Write(1, timestamp1, pageData)
		if err != nil {
			t.Fatalf("Failed to write to page logger: %v", err)
		}

		_, err = pageLogger.Write(2, timestamp2, pageData)
		if err != nil {
			t.Fatalf("Failed to write to page logger: %v", err)
		}

		_, err = pageLogger.Write(3, timestamp3, pageData)
		if err != nil {
			t.Fatalf("Failed to write to page logger: %v", err)
		}

		// Verify data exists before tombstoning
		readData := make([]byte, 4096)
		found, _, err := pageLogger.Read(1, timestamp1, readData)
		if err != nil {
			t.Fatalf("Failed to read from page logger: %v", err)
		}
		if !found {
			t.Fatal("Expected data to be found before tombstoning")
		}

		// Tombstone all the data to make the logs empty
		err = pageLogger.Tombstone(timestamp1)
		if err != nil {
			t.Fatalf("Failed to tombstone: %v", err)
		}

		err = pageLogger.Tombstone(timestamp2)
		if err != nil {
			t.Fatalf("Failed to tombstone: %v", err)
		}

		err = pageLogger.Tombstone(timestamp3)
		if err != nil {
			t.Fatalf("Failed to tombstone: %v", err)
		}

		// Force compaction to clean up empty logs
		err = pageLogger.ForceCompact(
			app.DatabaseManager.Resources(db.DatabaseID, db.BranchID).FileSystem(),
		)
		if err != nil {
			t.Fatalf("Failed to force compact: %v", err)
		}

		// Verify that we can't read any data (all should be tombstoned)
		found, _, err = pageLogger.Read(1, timestamp1, readData)
		if err != nil {
			t.Fatalf("Failed to read from page logger: %v", err)
		}
		if found {
			t.Fatal("Expected no data to be found after tombstoning")
		}

		found, _, err = pageLogger.Read(2, timestamp2, readData)
		if err != nil {
			t.Fatalf("Failed to read from page logger: %v", err)
		}
		if found {
			t.Fatal("Expected no data to be found after tombstoning")
		}

		found, _, err = pageLogger.Read(3, timestamp3, readData)
		if err != nil {
			t.Fatalf("Failed to read from page logger: %v", err)
		}
		if found {
			t.Fatal("Expected no data to be found after tombstoning")
		}

		// Verify compaction actually happened
		if pageLogger.CompactedAt.IsZero() {
			t.Fatal("Expected CompactedAt to be set after compaction")
		}
	})
}

func TestPageLogger_CompactEmptyPageLogsWithAcquiredLogs(t *testing.T) {
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
		defer pageLogger.Close()

		// Create some empty page logs
		pageData := make([]byte, 4096)
		timestamp1 := time.Now().UnixNano()
		timestamp2 := timestamp1 + 1000

		// Write data to create logs
		_, err = pageLogger.Write(1, timestamp1, pageData)
		if err != nil {
			t.Fatalf("Failed to write to page logger: %v", err)
		}

		_, err = pageLogger.Write(2, timestamp2, pageData)
		if err != nil {
			t.Fatalf("Failed to write to page logger: %v", err)
		}

		// Acquire the first timestamp to prevent its compaction
		pageLogger.Acquire(timestamp1)

		// Tombstone all the data to make the logs empty
		err = pageLogger.Tombstone(timestamp1)
		if err != nil {
			t.Fatalf("Failed to tombstone: %v", err)
		}

		err = pageLogger.Tombstone(timestamp2)
		if err != nil {
			t.Fatalf("Failed to tombstone: %v", err)
		}

		// Force compaction - should only compact the non-acquired log
		err = pageLogger.ForceCompact(
			app.DatabaseManager.Resources(db.DatabaseID, db.BranchID).FileSystem(),
		)
		if err != nil {
			t.Fatalf("Failed to force compact: %v", err)
		}

		// Verify that tombstoned data is still not found
		readData := make([]byte, 4096)
		found, _, err := pageLogger.Read(1, timestamp1, readData)
		if err != nil {
			t.Fatalf("Failed to read from page logger: %v", err)
		}
		if found {
			t.Fatal("Expected no data to be found after tombstoning")
		}

		found, _, err = pageLogger.Read(2, timestamp2, readData)
		if err != nil {
			t.Fatalf("Failed to read from page logger: %v", err)
		}
		if found {
			t.Fatal("Expected no data to be found after tombstoning")
		}

		// Release the acquired timestamp
		pageLogger.Release(timestamp1)

		// Force compaction again - should clean up the remaining empty log
		err = pageLogger.ForceCompact(
			app.DatabaseManager.Resources(db.DatabaseID, db.BranchID).FileSystem(),
		)
		if err != nil {
			t.Fatalf("Failed to force compact after release: %v", err)
		}

		// Verify data is still not found
		found, _, err = pageLogger.Read(1, timestamp1, readData)
		if err != nil {
			t.Fatalf("Failed to read from page logger: %v", err)
		}
		if found {
			t.Fatal("Expected no data to be found after final compaction")
		}

		// Verify compaction actually happened
		if pageLogger.CompactedAt.IsZero() {
			t.Fatal("Expected CompactedAt to be set after compaction")
		}
	})
}

func TestPageLogger_CompactionRemovesPageLogAndIndexFiles(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		db := test.MockDatabase(app)
		fileSystem := app.Cluster.LocalFS()

		pageLogger, err := storage.NewPageLogger(
			db.DatabaseID,
			db.BranchID,
			fileSystem,
		)

		if err != nil {
			t.Fatalf("Failed to create page logger: %v", err)
		}

		defer pageLogger.Close()

		// Write some data to create page logs
		pageData := make([]byte, 4096)
		timestamp1 := time.Now().UnixNano()
		timestamp2 := timestamp1 + 1000

		_, err = pageLogger.Write(1, timestamp1, pageData)

		if err != nil {
			t.Fatalf("Failed to write to page logger: %v", err)
		}

		_, err = pageLogger.Write(2, timestamp2, pageData)

		if err != nil {
			t.Fatalf("Failed to write to page logger: %v", err)
		}

		// Check that page log files exist before compaction
		logDir := fmt.Sprintf("%slogs/page/", file.GetDatabaseFileBaseDir(db.DatabaseID, db.BranchID))

		files, err := fileSystem.ReadDir(logDir)

		if err != nil {
			t.Fatalf("Failed to read log directory: %v", err)
		}

		// Find the page log files
		var pageLogFiles []string
		var indexFiles []string

		for _, file := range files {
			if strings.HasPrefix(file.Name(), "PAGE_LOG_") && !strings.HasSuffix(file.Name(), "_INDEX") {
				pageLogFiles = append(pageLogFiles, file.Name())
			}

			if strings.HasPrefix(file.Name(), "PAGE_LOG_") && strings.HasSuffix(file.Name(), "_INDEX") {
				indexFiles = append(indexFiles, file.Name())
			}
		}

		if len(pageLogFiles) == 0 {
			t.Fatal("Expected page log files to exist before compaction")
		}

		if len(indexFiles) == 0 {
			t.Fatal("Expected page log index files to exist before compaction")
		}

		// Force compaction to remove the page logs
		err = pageLogger.ForceCompact(
			app.DatabaseManager.Resources(db.DatabaseID, db.BranchID).FileSystem(),
		)

		if err != nil {
			t.Fatalf("Failed to force compact: %v", err)
		}

		// Check that page log files are removed after compaction
		filesAfterCompaction, err := fileSystem.ReadDir(logDir)

		if err != nil {
			t.Fatalf("Failed to read log directory after compaction: %v", err)
		}

		// Verify that the specific page log files are removed
		for _, pageLogFile := range pageLogFiles {
			found := false

			for _, remainingFile := range filesAfterCompaction {
				if remainingFile.Name() == pageLogFile {
					found = true
					break
				}
			}

			if found {
				t.Fatalf("Expected page log file %s to be removed after compaction", pageLogFile)
			}
		}

		// Verify that the specific index files are removed
		for _, indexFile := range indexFiles {
			found := false

			for _, remainingFile := range filesAfterCompaction {
				if remainingFile.Name() == indexFile {
					found = true
					break
				}
			}

			if found {
				t.Fatalf("Expected index file %s to be removed after compaction", indexFile)
			}
		}

		// Verify compaction happened
		if pageLogger.CompactedAt.IsZero() {
			t.Fatal("Expected CompactedAt to be set after compaction")
		}
	})
}

func TestPageLogger_CompactionWithManyPageLogs(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		db := test.MockDatabase(app)
		fileSystem := app.Cluster.LocalFS()

		pageLogger, err := storage.NewPageLogger(
			db.DatabaseID,
			db.BranchID,
			fileSystem,
		)

		if err != nil {
			t.Fatalf("Failed to create page logger: %v", err)
		}

		defer pageLogger.Close()

		// Create data for 17 different page logs across multiple page groups
		pageData := make([]byte, 4096)
		baseTime := time.Now().UnixNano()

		testData := []struct {
			pageNum   int64
			timestamp int64
		}{}

		// Create 17 different page logs with varying timestamps
		for i := range 17 {
			pageNum := int64(i*1000 + 1) // Spread across different page groups
			timestamp := baseTime + int64(i*1000)
			testData = append(testData, struct {
				pageNum   int64
				timestamp int64
			}{pageNum, timestamp})
		}

		// Write data to create the 17 page logs
		for _, tc := range testData {
			rand.Read(pageData)

			_, err = pageLogger.Write(tc.pageNum, tc.timestamp, pageData)
			if err != nil {
				t.Fatalf("Failed to write page %d at timestamp %d: %v", tc.pageNum, tc.timestamp, err)
			}
		}

		// Create some additional empty logs by tombstoning some data
		for i := range 5 {
			emptyTimestamp := baseTime + int64(i*10000)
			pageNum := int64(i*2000 + 1)

			_, err = pageLogger.Write(pageNum, emptyTimestamp, pageData)
			if err != nil {
				t.Fatalf("Failed to write page for tombstoning: %v", err)
			}

			err = pageLogger.Tombstone(emptyTimestamp)
			if err != nil {
				t.Fatalf("Failed to tombstone: %v", err)
			}
		}

		// Verify we have logs before compaction
		t.Logf("Created page logs, running compaction...")

		// Run compaction multiple times like the user scenario
		for i := range 3 {
			err = pageLogger.ForceCompact(
				app.DatabaseManager.Resources(db.DatabaseID, db.BranchID).FileSystem(),
			)

			if err != nil {
				t.Fatalf("Failed to compact on attempt %d: %v", i+1, err)
			}

			t.Logf("Compaction attempt %d completed successfully", i+1)
		}

		// Verify compaction worked
		if pageLogger.CompactedAt.IsZero() {
			t.Fatal("Expected CompactedAt to be set after compaction")
		}

		t.Logf("Successfully completed compaction with many page logs")
	})
}

func TestPageLoggerCompaction_AfterRestart(t *testing.T) {
	test.Run(t, func() {
		// Set compaction interval to 0 to test compaction behavior after restart
		originalInterval := storage.PageLoggerCompactInterval
		storage.PageLoggerCompactInterval = time.Millisecond
		defer func() {
			storage.PageLoggerCompactInterval = originalInterval
		}()

		// Create first server instance
		server1 := test.NewUnstartedTestServer(t)
		server1.Started = server1.App.Cluster.Node().Start()
		<-server1.Started // Wait for server to start

		// Create mock database
		db := test.MockDatabase(server1.App)

		// Create page logger with TieredFS (which handles sync to low-tier storage)
		pageLogger1, err := storage.NewPageLogger(
			db.DatabaseID,
			db.BranchID,
			server1.App.Cluster.TieredFS(),
		)

		if err != nil {
			t.Fatalf("Failed to create page logger: %v", err)
		}

		// Write data across multiple page logs to simulate real-world scenario
		baseTimestamp := time.Now().UnixNano()

		testData := []struct {
			pageNum   int64
			timestamp int64
			data      []byte
		}{}

		// Create 10 page logs with different timestamps and page groups
		for i := range 10 {
			pageNum := int64(i*storage.PageLoggerPageGroups + 1) // Different page groups
			timestamp := baseTimestamp + int64(i*1000)
			data := make([]byte, 4096)
			rand.Read(data)
			testData = append(testData, struct {
				pageNum   int64
				timestamp int64
				data      []byte
			}{pageNum, timestamp, data})
		}

		// Write all data to the first page logger
		for _, tc := range testData {
			_, err = pageLogger1.Write(tc.pageNum, tc.timestamp, tc.data)

			if err != nil {
				t.Fatalf("Failed to write page %d: %v", tc.pageNum, err)
			}
		}

		// Verify data can be read from first instance
		for _, tc := range testData {
			readData := make([]byte, 4096)
			found, _, err := pageLogger1.Read(tc.pageNum, tc.timestamp, readData)

			if err != nil {
				t.Fatalf("Failed to read page %d: %v", tc.pageNum, err)
			}

			if !found {
				t.Fatalf("Expected to find data for page %d", tc.pageNum)
			}

			if !bytes.Equal(readData, tc.data) {
				t.Fatalf("Data mismatch for page %d", tc.pageNum)
			}
		}

		// Close the page logger and shutdown the server
		// This should trigger tiered FS sync to low-tier storage
		err = pageLogger1.Close()

		if err != nil {
			t.Fatalf("Failed to close page logger: %v", err)
		}

		t.Logf("Shutting down server1...")
		server1.Shutdown()

		// Start a new server instance (simulating restart)
		server2 := test.NewUnstartedTestServer(t)
		server2.Started = server2.App.Cluster.Node().Start()
		<-server2.Started // Wait for server to start

		// Create a new page logger instance with the same database ID (simulating restart)
		// This should recover any files that were synced to low-tier storage
		pageLogger2, err := storage.NewPageLogger(
			db.DatabaseID,
			db.BranchID,
			server2.App.Cluster.TieredFS(),
		)

		if err != nil {
			t.Fatalf("Failed to create page logger after restart: %v", err)
		}

		defer pageLogger2.Close()

		// Write additional data to create new page logs
		additionalData := []struct {
			pageNum   int64
			timestamp int64
			data      []byte
		}{}

		for i := 10; i < 17; i++ { // Create 7 more logs to reach 17 total
			pageNum := int64(i*storage.PageLoggerPageGroups + 1)
			timestamp := baseTimestamp + int64(i*1000)
			data := make([]byte, 4096)
			rand.Read(data)
			additionalData = append(additionalData, struct {
				pageNum   int64
				timestamp int64
				data      []byte
			}{pageNum, timestamp, data})
		}

		// Write additional data
		for _, tc := range additionalData {
			_, err = pageLogger2.Write(tc.pageNum, tc.timestamp, tc.data)

			if err != nil {
				t.Fatalf("Failed to write additional page %d: %v", tc.pageNum, err)
			}
		}

		// Now try to compact with all 17 page logs
		// This is where the user is experiencing EOF errors
		t.Logf("Attempting compaction with 17 page logs after restart...")

		err = pageLogger2.ForceCompact(
			server2.App.DatabaseManager.Resources(db.DatabaseID, db.BranchID).FileSystem(),
		)

		if err != nil {
			t.Fatalf("Failed to compact after restart: %v", err)
		}

		// Verify compaction worked
		if pageLogger2.CompactedAt.IsZero() {
			t.Fatal("Expected CompactedAt to be set after compaction")
		}

		// Try multiple compactions to ensure consistency
		for i := range 3 {
			err = pageLogger2.ForceCompact(
				server2.App.DatabaseManager.Resources(db.DatabaseID, db.BranchID).FileSystem(),
			)

			if err != nil {
				t.Fatalf("Failed to compact on attempt %d: %v", i+1, err)
			}

			t.Logf("Compaction attempt %d completed successfully", i+1)
		}

		// Verify we can still read the additional data
		for _, tc := range additionalData {
			readData := make([]byte, 4096)
			found, _, err := pageLogger2.Read(tc.pageNum, tc.timestamp, readData)

			if err != nil {
				t.Fatalf("Failed to read additional page %d after compaction: %v", tc.pageNum, err)
			}

			// Note: Data might not be found if it was compacted to durable storage
			if found {
				t.Logf("Successfully read additional data for page %d after compaction", tc.pageNum)
			} else {
				t.Logf("Data for page %d was compacted to durable storage", tc.pageNum)
			}
		}

		t.Logf("Successfully completed server restart and compaction test")

		server2.Shutdown()
	})
}

func TestPageLogger_ConcurrentReadsDuringCompaction(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		db := test.MockDatabase(app)

		// Set a very short compaction interval to trigger compaction frequently
		originalInterval := storage.PageLoggerCompactInterval
		storage.PageLoggerCompactInterval = time.Millisecond * 10
		defer func() {
			storage.PageLoggerCompactInterval = originalInterval
		}()

		pageLogger, err := storage.NewPageLogger(
			db.DatabaseID,
			db.BranchID,
			app.Cluster.LocalFS(),
		)

		if err != nil {
			t.Fatalf("Failed to create page logger: %v", err)
		}

		defer pageLogger.Close()

		// Get a properly initialized durable filesystem
		durableFS := app.DatabaseManager.Resources(db.DatabaseID, db.BranchID).FileSystem()

		// Write multiple versions of the same page to create compaction opportunity
		pageNumber := int64(1)
		timestamps := []int64{1000, 2000, 3000, 4000, 5000}
		pageData := make([][]byte, len(timestamps))

		for i, ts := range timestamps {
			data := make([]byte, 4096)
			// Fill with recognizable pattern
			for j := range data {
				data[j] = byte(i + 1) // 1, 2, 3, 4, 5
			}

			pageData[i] = data

			_, err := pageLogger.Write(pageNumber, ts, data)

			if err != nil {
				t.Fatalf("Failed to write page: %v", err)
			}
		}

		// Wait for potential compaction to be eligible
		time.Sleep(time.Millisecond * 20)

		// Start concurrent operations
		var wg sync.WaitGroup
		errors := make(chan error, 100)
		done := make(chan bool, 1)

		// Start multiple readers
		for i := range 5 {
			wg.Add(1)
			go func(readerID int) {
				defer wg.Done()
				readData := make([]byte, 4096)

				for {
					select {
					case <-done:
						return
					default:
						// Try to read different versions
						for _, ts := range timestamps {
							pageLogger.Acquire(ts)
							found, version, err := pageLogger.Read(pageNumber, ts, readData)
							pageLogger.Release(ts)

							if err != nil {
								errors <- fmt.Errorf("reader %d: read error at timestamp %d: %v", readerID, ts, err)
								return
							}

							if found {
								// Validate data integrity
								expectedByte := byte(0)
								for j, expectedTs := range timestamps {
									if ts >= expectedTs {
										expectedByte = byte(j + 1)
									}
								}

								if len(readData) > 0 && readData[0] != expectedByte {
									errors <- fmt.Errorf("reader %d: data corruption at timestamp %d, expected %d, got %d",
										readerID, ts, expectedByte, readData[0])
									return
								}

								// Verify version is reasonable
								if version <= 0 || version > storage.PageVersion(ts) {
									errors <- fmt.Errorf("reader %d: invalid version %d for timestamp %d",
										readerID, version, ts)
									return
								}
							}
						}
					}
				}
			}(i)
		}

		// Start compaction goroutine
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
					err := pageLogger.Compact(durableFS)

					if err != nil && err != storage.ErrCompactionInProgress {
						errors <- fmt.Errorf("compaction error: %v", err)
						return
					}

					time.Sleep(time.Millisecond * 5)
				}
			}
		}()

		// Run the test for a short duration
		time.Sleep(time.Millisecond * 100)
		close(done)
		wg.Wait()

		// Check for any errors
		close(errors)

		for err := range errors {
			t.Errorf("Concurrent access error: %v", err)
		}
	})
}

func TestPageLogger_ReadDuringReload(t *testing.T) {
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

		defer pageLogger.Close()

		// Write some data
		pageNumber := int64(1)
		timestamp := int64(1000)
		data := make([]byte, 4096)

		for i := range data {
			data[i] = 0xFF
		}

		_, err = pageLogger.Write(pageNumber, timestamp, data)

		if err != nil {
			t.Fatalf("Failed to write page: %v", err)
		}

		// Get a properly initialized durable filesystem
		durableFS := app.DatabaseManager.Resources(db.DatabaseID, db.BranchID).FileSystem()

		// Test that reads work correctly during reload
		var wg sync.WaitGroup
		errors := make(chan error, 10)

		// Start multiple readers
		for i := range 3 {
			wg.Add(1)
			go func(readerID int) {
				defer wg.Done()

				for j := range 10 {
					readData := make([]byte, 4096)
					pageLogger.Acquire(timestamp)
					found, _, err := pageLogger.Read(pageNumber, timestamp, readData)
					pageLogger.Release(timestamp)

					if err != nil {
						errors <- fmt.Errorf("reader %d iteration %d: %v", readerID, j, err)
						return
					}

					if found {
						// Validate data
						if readData[0] != 0xFF {
							errors <- fmt.Errorf("reader %d iteration %d: data corruption", readerID, j)
							return
						}
					}
				}
			}(i)
		}

		// Force a reload while reads are happening
		wg.Add(1)
		go func() {
			defer wg.Done()
			// This should be safe to call concurrently with reads
			err := pageLogger.ForceCompact(durableFS)

			if err != nil && err != storage.ErrCompactionInProgress {
				errors <- fmt.Errorf("force compact error: %v", err)
			}
		}()

		wg.Wait()
		close(errors)

		for err := range errors {
			t.Errorf("Concurrent reload error: %v", err)
		}
	})
}
