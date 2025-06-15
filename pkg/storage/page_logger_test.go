package storage_test

import (
	"bytes"
	"crypto/rand"
	"sync"
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/storage"
	"github.com/litebase/litebase/server"
)

func TestNewPageLogger(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		db := test.MockDatabase(app)

		pageLogger, err := storage.NewPageLogger(
			db.DatabaseId,
			db.BranchId,
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
		storage.PageLoggerCompactInterval = 0

		defer func() {
			storage.PageLoggerCompactInterval = storage.DefaultPageLoggerCompactInterval
		}()

		pageLogger, err := storage.NewPageLogger(
			db.DatabaseId,
			db.BranchId,
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
				app.DatabaseManager.Resources(db.DatabaseId, db.BranchId).FileSystem(),
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
			db.DatabaseId,
			db.BranchId,
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
			db.DatabaseId,
			db.BranchId,
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
			app.DatabaseManager.Resources(db.DatabaseId, db.BranchId).FileSystem(),
		)

		if err != nil {
			t.Fatalf("Failed to compact page logger: %v", err)
		}
	})
}

func TestPageLogger_Read(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		db := test.MockDatabase(app)

		pageLogger, err := storage.NewPageLogger(
			db.DatabaseId,
			db.BranchId,
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
			db.DatabaseId,
			db.BranchId,
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
			app.DatabaseManager.Resources(db.DatabaseId, db.BranchId).FileSystem(),
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
		db := test.MockDatabase(app)

		pageLogger, err := storage.NewPageLogger(
			db.DatabaseId,
			db.BranchId,
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

		// Compaction will not run because of the compaction interval has not passed
		err = pageLogger.Compact(
			app.DatabaseManager.Resources(db.DatabaseId, db.BranchId).FileSystem(),
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
			db.DatabaseId,
			db.BranchId,
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
			app.DatabaseManager.Resources(db.DatabaseId, db.BranchId).FileSystem(),
		)

		if !pageLogger.CompactedAt.IsZero() {
			t.Fatal("Expected CompactedAt to be zero, but got non-zero value")
		}

		pageLogger.Release(1)

		pageLogger.Compact(
			app.DatabaseManager.Resources(db.DatabaseId, db.BranchId).FileSystem(),
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
			db.DatabaseId,
			db.BranchId,
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
			db.DatabaseId,
			db.BranchId,
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
			db.DatabaseId,
			db.BranchId,
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
			app.DatabaseManager.Resources(db.DatabaseId, db.BranchId).FileSystem(),
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
			db.DatabaseId,
			db.BranchId,
			app.Cluster.LocalFS(),
		)

		if err != nil {
			t.Fatalf("Failed to create page logger: %v", err)
		}

		err = pageLogger.Compact(
			app.DatabaseManager.Resources(db.DatabaseId, db.BranchId).FileSystem(),
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
			db.DatabaseId,
			db.BranchId,
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
					app.DatabaseManager.Resources(db.DatabaseId, db.BranchId).FileSystem(),
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
						app.DatabaseManager.Resources(db.DatabaseId, db.BranchId).FileSystem(),
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
		// 	db.DatabaseId,
		// 	db.BranchId,
		// 	app.Cluster.LocalFS(),
		// )

		// if err != nil {
		// 	t.Fatalf("Failed to create page logger: %v", err)
		// }

		// err = pageLogger.Compact(
		// 	app.DatabaseManager.Resources(db.DatabaseId, db.BranchId).FileSystem(),
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
			db.DatabaseId,
			db.BranchId,
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
			db.DatabaseId,
			db.BranchId,
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
			db.DatabaseId,
			db.BranchId,
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
