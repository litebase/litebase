package storage_test

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/config"
	"github.com/litebase/litebase/pkg/server"
	"github.com/litebase/litebase/pkg/sqlite3"
	"github.com/litebase/litebase/pkg/storage"
)

func TestNewDurableDatabaseFileSystem(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mockDatabase := test.MockDatabase(app)

		dfs := storage.NewDurableDatabaseFileSystem(
			app.Cluster.LocalFS(),
			app.Cluster.LocalFS(),
			app.DatabaseManager.PageLogManager().Get(mockDatabase.DatabaseID, mockDatabase.BranchID, app.Cluster.LocalFS()),
			config.StorageModeLocal,
			mockDatabase.DatabaseID,
			mockDatabase.BranchID,
			4096,
		)

		if dfs == nil {
			t.Error("expected local database file system, got nil")
		}
	})
}

func TestDurableDatabaseFileSystem_Compact(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mockDatabase := test.MockDatabase(app)

		storage.PageLoggerCompactInterval = 0
		defer func() {
			storage.PageLoggerCompactInterval = storage.DefaultPageLoggerCompactInterval
		}()

		dfs := storage.NewDurableDatabaseFileSystem(
			app.Cluster.TieredFS(),
			app.Cluster.NetworkFS(),
			app.DatabaseManager.PageLogManager().Get(mockDatabase.DatabaseID, mockDatabase.BranchID, app.Cluster.NetworkFS()),
			config.StorageModeLocal,
			mockDatabase.DatabaseID,
			mockDatabase.BranchID,
			4096,
		)

		// Test parameters
		const numWrites = 100
		const numCompactionCycles = 3
		const pageSize = 4096

		// Map to store all written data for verification
		writtenData := make(map[int64][]byte)
		// baseTimestamp := time.Now().UTC().UnixNano()

		for cycle := range numCompactionCycles {
			t.Logf("Starting compaction cycle %d", cycle+1)

			// Phase 1: Write data and verify immediate reads
			t.Logf("Phase 1: Writing %d pages and verifying immediate reads", numWrites)

			for i := range numWrites {
				offset := int64(i * pageSize)
				// timestamp := baseTimestamp + int64(cycle*numWrites+i)
				timestamp := time.Now().UTC().UnixNano()

				// Generate unique data for this write
				data := make([]byte, pageSize)
				copy(data[0:8], fmt.Appendf(nil, "cycle%d", cycle))
				copy(data[8:16], fmt.Appendf(nil, "write%03d", i))
				rand.Read(data[16:]) // Fill rest with random data

				// Store expected data
				writtenData[offset] = make([]byte, pageSize)
				copy(writtenData[offset], data)

				// Write to DFS
				n, err := dfs.WriteAt(timestamp, timestamp, data, offset)
				if err != nil {
					t.Fatalf("Failed to write at offset %d: %v", offset, err)
				}

				if n != pageSize {
					t.Fatalf("Expected to write %d bytes, wrote %d", pageSize, n)
				}

				// Immediately read back and verify
				readBuffer := make([]byte, pageSize)
				n, err = dfs.ReadAt(timestamp, timestamp, readBuffer, offset, pageSize)

				if err != nil {
					t.Fatalf("Failed to read at offset %d: %v", offset, err)
				}

				if n != pageSize {
					t.Fatalf("Expected to read %d bytes, read %d", pageSize, n)
				}

				// Verify data matches
				if !bytes.Equal(readBuffer, data) {
					t.Fatalf("Data mismatch at offset %d after immediate read in cycle %d", offset, cycle)
				}
			}

			t.Logf("Phase 1 complete: All %d writes verified successfully", numWrites)

			// Phase 2: Verify all previous data is still readable before compaction
			t.Logf("Phase 2: Verifying all previous data before compaction")

			for offset, expectedData := range writtenData {
				readBuffer := make([]byte, pageSize)
				// timestamp := baseTimestamp + int64(len(writtenData)) // Use latest timestamp
				timestamp := time.Now().UTC().UnixNano()

				n, err := dfs.ReadAt(timestamp, timestamp, readBuffer, offset, pageSize)
				if err != nil {
					t.Fatalf("Failed to read at offset %d before compaction in cycle %d: %v", offset, cycle, err)
				}
				if n != pageSize {
					t.Fatalf("Expected to read %d bytes, read %d before compaction", pageSize, n)
				}

				if !bytes.Equal(readBuffer, expectedData) {
					t.Fatalf("Data corruption at offset %d before compaction in cycle %d", offset, cycle)
				}
			}
			t.Logf("Phase 2 complete: All %d pages verified before compaction", len(writtenData))

			// Phase 3: Compact
			t.Logf("Phase 3: Starting compaction")
			err := dfs.Compact()
			if err != nil {
				t.Fatalf("Compaction failed in cycle %d: %v", cycle, err)
			}
			t.Logf("Phase 3 complete: Compaction successful")

			// Phase 4: Verify all data after compaction
			t.Logf("Phase 4: Verifying all data after compaction")
			for offset, expectedData := range writtenData {
				readBuffer := make([]byte, pageSize)
				timestamp := time.Now().UTC().UnixNano() // Use fresh timestamp for post-compaction read

				n, err := dfs.ReadAt(timestamp, timestamp, readBuffer, offset, pageSize)
				if err != nil {
					t.Fatalf("Failed to read at offset %d after compaction in cycle %d: %v", offset, cycle, err)
				}
				if n != pageSize {
					t.Fatalf("Expected to read %d bytes, read %d after compaction", pageSize, n)
				}

				if !bytes.Equal(readBuffer, expectedData) {
					t.Fatalf("Data corruption at offset %d after compaction in cycle %d", offset, cycle)
				}
			}
			t.Logf("Phase 4 complete: All %d pages verified after compaction", len(writtenData))

			t.Logf("Cycle %d complete successfully", cycle+1)
		}

		t.Logf("All %d compaction cycles completed successfully with %d total writes", numCompactionCycles, len(writtenData))

	})
}

func TestDurableDatabaseFileSystem_CompactWithConcurrentWrites(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mockDatabase := test.MockDatabase(app)

		storage.PageLoggerCompactInterval = 0
		defer func() {
			storage.PageLoggerCompactInterval = storage.DefaultPageLoggerCompactInterval
		}()

		dfs := storage.NewDurableDatabaseFileSystem(
			app.Cluster.TieredFS(),
			app.Cluster.NetworkFS(),
			app.DatabaseManager.PageLogManager().Get(mockDatabase.DatabaseID, mockDatabase.BranchID, app.Cluster.NetworkFS()),
			config.StorageModeLocal,
			mockDatabase.DatabaseID,
			mockDatabase.BranchID,
			4096,
		)

		// Test parameters
		const numWrites = 1000
		const numCompactionCycles = 10
		const numConcurrentWrites = 50
		const pageSize = 4096

		// Map to store all written data for verification
		writtenData := make(map[int64][]byte)

		for cycle := range numCompactionCycles {
			t.Logf("Starting compaction cycle %d", cycle+1)

			// Phase 1: Write initial data and verify immediate reads
			t.Logf("Phase 1: Writing %d pages and verifying immediate reads", numWrites)

			for i := range numWrites {
				offset := int64(i * pageSize)
				// timestamp := baseTimestamp + int64(cycle*numWrites+i)
				timestamp := time.Now().UTC().UnixNano()

				// Generate unique data for this write
				data := make([]byte, pageSize)
				copy(data[0:8], fmt.Appendf(nil, "cycle%d", cycle))
				copy(data[8:16], fmt.Appendf(nil, "write%03d", i))
				rand.Read(data[16:]) // Fill rest with random data

				// Store expected data
				writtenData[offset] = make([]byte, pageSize)
				copy(writtenData[offset], data)

				// Write to DFS
				n, err := dfs.WriteAt(timestamp, timestamp, data, offset)
				if err != nil {
					t.Fatalf("Failed to write at offset %d: %v", offset, err)
				}

				if n != pageSize {
					t.Fatalf("Expected to write %d bytes, wrote %d", pageSize, n)
				}

				// Immediately read back and verify
				readBuffer := make([]byte, pageSize)
				n, err = dfs.ReadAt(timestamp, timestamp, readBuffer, offset, pageSize)

				if err != nil {
					t.Fatalf("Failed to read at offset %d: %v", offset, err)
				}

				if n != pageSize {
					t.Fatalf("Expected to read %d bytes, read %d", pageSize, n)
				}

				// Verify data matches
				if !bytes.Equal(readBuffer, data) {
					t.Fatalf("Data mismatch at offset %d after immediate read in cycle %d", offset, cycle)
				}
			}

			t.Logf("Phase 1 complete: All %d writes verified successfully", numWrites)

			// Phase 2: Start concurrent writes during compaction
			t.Logf("Phase 2: Starting compaction with %d concurrent writes", numConcurrentWrites)

			// Channel to signal completion of concurrent writes
			writeDone := make(chan error, numConcurrentWrites)
			compactDone := make(chan error, 1)

			// Start concurrent writes
			for i := range numConcurrentWrites {
				go func(writeIndex int) {
					// Use unique offsets for concurrent writes to avoid conflicts
					offset := int64((numWrites + writeIndex) * pageSize)
					timestamp := time.Now().UTC().UnixNano()

					// Generate unique data for concurrent write
					data := make([]byte, pageSize)
					copy(data[0:8], fmt.Appendf(nil, "conc%d", cycle))
					copy(data[8:16], fmt.Appendf(nil, "wrt%03d", writeIndex))
					rand.Read(data[16:])

					// Write to DFS
					n, err := dfs.WriteAt(timestamp, timestamp, data, offset)
					if err != nil {
						writeDone <- fmt.Errorf("concurrent write %d failed at offset %d: %v", writeIndex, offset, err)
						return
					}

					if n != pageSize {
						writeDone <- fmt.Errorf("concurrent write %d: expected to write %d bytes, wrote %d", writeIndex, pageSize, n)
						return
					}

					// Store the data for later verification
					writtenData[offset] = make([]byte, pageSize)
					copy(writtenData[offset], data)

					writeDone <- nil
				}(i)
			}

			// Start compaction in a separate goroutine
			go func() {
				err := dfs.Compact()
				compactDone <- err
			}()

			// Wait for all concurrent writes to complete
			var writeErrors []error
			for i := 0; i < numConcurrentWrites; i++ {
				if err := <-writeDone; err != nil {
					writeErrors = append(writeErrors, err)
				}
			}

			// Check for write errors
			if len(writeErrors) > 0 {
				for _, err := range writeErrors {
					t.Errorf("Concurrent write error: %v", err)
				}
				t.Fatalf("Failed concurrent writes during compaction in cycle %d", cycle)
			}

			// Wait for compaction to complete
			if err := <-compactDone; err != nil {
				t.Fatalf("Compaction failed in cycle %d: %v", cycle, err)
			}

			t.Logf("Phase 2 complete: Compaction and %d concurrent writes successful", numConcurrentWrites)

			// Phase 3: Verify all data after compaction and concurrent writes
			t.Logf("Phase 3: Verifying all data after compaction and concurrent writes")
			for offset, expectedData := range writtenData {
				readBuffer := make([]byte, pageSize)
				timestamp := time.Now().UTC().UnixNano()

				n, err := dfs.ReadAt(timestamp, timestamp, readBuffer, offset, pageSize)
				if err != nil {
					t.Fatalf("Failed to read at offset %d after compaction in cycle %d: %v", offset, cycle, err)
				}
				if n != pageSize {
					t.Fatalf("Expected to read %d bytes, read %d after compaction", pageSize, n)
				}

				if !bytes.Equal(readBuffer, expectedData) {
					t.Fatalf("Data corruption at offset %d after compaction in cycle %d", offset, cycle)
				}
			}
			t.Logf("Phase 3 complete: All %d pages verified after compaction and concurrent writes", len(writtenData))

			t.Logf("Cycle %d complete successfully", cycle+1)
		}

		t.Logf("All %d compaction cycles completed successfully with %d total writes (%d concurrent)",
			numCompactionCycles, len(writtenData), numCompactionCycles*numConcurrentWrites)
	})
}

func TestDurableDatabaseFileSystem_Compact_WithDatabaseConnection(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mockDatabase := test.MockDatabase(app)

		con, err := app.DatabaseManager.ConnectionManager().Get(mockDatabase.DatabaseID, mockDatabase.BranchID)

		if err != nil {
			t.Fatal("expected nil, got", err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(con)

		// Create a test table
		_, err = con.GetConnection().Exec("CREATE TABLE IF NOT EXISTS test (id INTEGER PRIMARY KEY, value TEXT, created_at TEXT, updated_at TEXT)", nil)

		if err != nil {
			t.Fatal("expected nil, got", err)
		}

		rounds := 20
		inserts := 50000
		expectedCount := 0

		for range rounds {
			for range inserts {
				_, err = con.GetConnection().Exec("INSERT INTO test (value, created_at, updated_at) VALUES (?, ?, ?)", []sqlite3.StatementParameter{
					{
						Type:  sqlite3.ParameterTypeText,
						Value: fmt.Appendf(nil, "value-%s", uuid.NewString()),
					},
					{
						Type:  sqlite3.ParameterTypeText,
						Value: []byte(time.Now().UTC().Format(time.RFC3339)),
					},
					{
						Type:  sqlite3.ParameterTypeText,
						Value: []byte(time.Now().UTC().Format(time.RFC3339)),
					},
				})

				if err != nil {
					t.Fatal("expected nil, got", err)
				}

				expectedCount++
			}

			result, err := con.GetConnection().Exec("SELECT COUNT(*) FROM test", nil)

			if err != nil {
				t.Fatal("expected nil, got", err)
			}

			// time.Sleep(1 * time.Second)

			count := int(result.Rows[0][0].Int64())

			if count != expectedCount {
				t.Fatalf("expected %d, got %d", expectedCount, count)
			}

			// con.GetConnection().Checkpoint()
			// go func() {
			// 	con.GetConnection().FileSystem().Compact()
			// }()

			// Pragma integrity check
			result, err = con.GetConnection().Exec("PRAGMA integrity_check", nil)

			if err != nil {
				t.Fatal("expected nil, got", err)
			}

			if len(result.Rows) != 1 || !bytes.Equal(result.Rows[0][0].Text(), []byte("ok")) {
				t.Fatal("expected integrity_check to return 'ok', got", string(result.Rows[0][0].Text()))
			}

		}

		t.Logf("Final row count: %d", expectedCount)
	})
}

func TestDurableDatabaseFileSystem_FileSystem(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mockDatabase := test.MockDatabase(app)

		dfs := storage.NewDurableDatabaseFileSystem(
			app.Cluster.LocalFS(),
			app.Cluster.LocalFS(),
			app.DatabaseManager.PageLogManager().Get(mockDatabase.DatabaseID, mockDatabase.BranchID, app.Cluster.LocalFS()),
			config.StorageModeLocal,
			mockDatabase.DatabaseID,
			mockDatabase.BranchID,
			4096,
		)

		fs := dfs.FileSystem()

		if fs == nil {
			t.Error("expected file system, got nil")
		}

		if fs != app.Cluster.LocalFS() {
			t.Error("expected local file system, got", fs)
		}
	})
}

func TestDurableDatabaseFileSystem_ForceCompact(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mockDatabase := test.MockDatabase(app)

		storage.PageLoggerCompactInterval = 0
		defer func() {
			storage.PageLoggerCompactInterval = storage.DefaultPageLoggerCompactInterval
		}()

		dfs := app.DatabaseManager.Resources(
			mockDatabase.DatabaseID,
			mockDatabase.BranchID,
		).FileSystem()

		err := dfs.ForceCompact()

		if err != nil {
			t.Error("expected nil, got", err)
		}
	})
}

func TestDurableDatabaseFileSystem_GetRangeFile(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mockDatabase := test.MockDatabase(app)

		dfs := storage.NewDurableDatabaseFileSystem(
			app.Cluster.LocalFS(),
			app.Cluster.LocalFS(),
			app.DatabaseManager.PageLogManager().Get(mockDatabase.DatabaseID, mockDatabase.BranchID, app.Cluster.LocalFS()),
			config.StorageModeLocal,
			mockDatabase.DatabaseID,
			mockDatabase.BranchID,
			4096,
		)

		rangeFile, err := dfs.GetRangeFile(1)

		if err != nil {
			t.Error("expected nil, got", err)
		}

		if rangeFile == nil {
			t.Error("expected range file, got nil")
		}
	})
}

func TestDurableDatabaseFileSystem_Metadata(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mockDatabase := test.MockDatabase(app)

		dfs := storage.NewDurableDatabaseFileSystem(
			app.Cluster.LocalFS(),
			app.Cluster.LocalFS(),
			app.DatabaseManager.PageLogManager().Get(mockDatabase.DatabaseID, mockDatabase.BranchID, app.Cluster.LocalFS()),
			config.StorageModeLocal,
			mockDatabase.DatabaseID,
			mockDatabase.BranchID,
			4096,
		)

		metadata := dfs.Metadata()

		if metadata == nil {
			t.Error("expected metadata, got nil")
		}
	})
}

func TestDurableDatabaseFileSystem_Open(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mockDatabase := test.MockDatabase(app)

		dfs := storage.NewDurableDatabaseFileSystem(
			app.Cluster.LocalFS(),
			app.Cluster.LocalFS(),
			app.DatabaseManager.PageLogManager().Get(mockDatabase.DatabaseID, mockDatabase.BranchID, app.Cluster.LocalFS()),
			config.StorageModeLocal,
			mockDatabase.DatabaseID,
			mockDatabase.BranchID,
			4096,
		)

		file, err := dfs.Open("test.db")

		if err != nil {
			t.Error("expected nil, got", err)
		}

		if file != nil {
			t.Error("expected nil, got", file)
		}
	})
}

func TestDurableDatabaseFileSystem_PageSize(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mockDatabase := test.MockDatabase(app)

		dfs := storage.NewDurableDatabaseFileSystem(
			app.Cluster.LocalFS(),
			app.Cluster.LocalFS(),
			app.DatabaseManager.PageLogManager().Get(mockDatabase.DatabaseID, mockDatabase.BranchID, app.Cluster.LocalFS()),
			config.StorageModeLocal,
			mockDatabase.DatabaseID,
			mockDatabase.BranchID,
			4096,
		)

		pageSize := dfs.PageSize()

		if pageSize != 4096 {
			t.Error("expected 4096, got", pageSize)
		}
	})
}

func TestDurableDatabaseFileSystem_Path(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mockDatabase := test.MockDatabase(app)

		dfs := storage.NewDurableDatabaseFileSystem(
			app.Cluster.LocalFS(),
			app.Cluster.LocalFS(),
			app.DatabaseManager.PageLogManager().Get(mockDatabase.DatabaseID, mockDatabase.BranchID, app.Cluster.LocalFS()),
			config.StorageModeLocal,
			mockDatabase.DatabaseID,
			mockDatabase.BranchID,
			4096,
		)

		path := dfs.Path()

		if path != config.StorageModeLocal {
			t.Error("expected local, got", path)
		}
	})
}

func TestDurableDatabaseFileSystem_ReadAt(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mockDatabase := test.MockDatabase(app)

		dfs := storage.NewDurableDatabaseFileSystem(
			app.Cluster.LocalFS(),
			app.Cluster.LocalFS(),
			app.DatabaseManager.PageLogManager().Get(mockDatabase.DatabaseID, mockDatabase.BranchID, app.Cluster.LocalFS()),
			config.StorageModeLocal,
			mockDatabase.DatabaseID,
			mockDatabase.BranchID,
			4096,
		)

		buffer := make([]byte, 4096)

		n, err := dfs.ReadAt(0, 0, buffer, 0, 4096)

		if err != nil {
			t.Error("expected nil, got", err)
		}

		if n != 0 {
			t.Error("expected 0, got", n)
		}

		data := make([]byte, 4096)

		rand.Read(data)

		// Write some data to the file
		n, err = dfs.WriteAt(int64(0), 0, data, 0)

		if err != nil {
			t.Error("expected nil, got", err)
		}

		if n != 4096 {
			t.Error("expected 4096, got", n)
		}

		// Read the data back
		n, err = dfs.ReadAt(0, 0, buffer, 0, 4096)

		if err != nil {
			t.Error("expected nil, got", err)
		}

		if n != 4096 {
			t.Error("expected 4096, got", n)
		}

		if !bytes.Equal(buffer[:n], data) {
			t.Error("expected test, got", string(buffer[:n]))
		}
	})
}

func TestDurableDatabaseFileSystem_SetWriteHook(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mockDatabase := test.MockDatabase(app)

		var offset int64
		var data []byte

		dfs := storage.NewDurableDatabaseFileSystem(
			app.Cluster.LocalFS(),
			app.Cluster.LocalFS(),
			app.DatabaseManager.PageLogManager().Get(mockDatabase.DatabaseID, mockDatabase.BranchID, app.Cluster.LocalFS()),
			config.StorageModeLocal,
			mockDatabase.DatabaseID,
			mockDatabase.BranchID,
			0,
		).SetWriteHook(func(o int64, d []byte) {
			offset = o
			data = d
		})

		if dfs == nil {
			t.Error("expected a database file system, got nil")
		}

		dfs.WriteHook(0, []byte("test"))

		if offset != 0 {
			t.Error("expected 0, got", offset)
		}

		if string(data) != "test" {
			t.Error("expected test, got", string(data))
		}
	})
}

func TestDurableDatabaseFileSystem_Size(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mockDatabase := test.MockDatabase(app)

		dfs := storage.NewDurableDatabaseFileSystem(
			app.Cluster.LocalFS(),
			app.Cluster.LocalFS(),
			app.DatabaseManager.PageLogManager().Get(mockDatabase.DatabaseID, mockDatabase.BranchID, app.Cluster.LocalFS()),
			config.StorageModeLocal,
			mockDatabase.DatabaseID,
			mockDatabase.BranchID,
			4096,
		)

		size, err := dfs.Size()

		if err != nil {
			t.Error("expected nil, got", err)
		}

		if size != 0 {
			t.Error("expected 0, got", size)
		}
	})
}

func TestDurableDatabaseFileSystemShutdown(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mockDatabase := test.MockDatabase(app)

		dfs := storage.NewDurableDatabaseFileSystem(
			app.Cluster.LocalFS(),
			app.Cluster.LocalFS(),
			app.DatabaseManager.PageLogManager().Get(mockDatabase.DatabaseID, mockDatabase.BranchID, app.Cluster.LocalFS()),
			config.StorageModeLocal,
			mockDatabase.DatabaseID,
			mockDatabase.BranchID,
			4096,
		)
		err := dfs.Shutdown()

		if err != nil {
			t.Error("expected nil, got", err)
		}
	})
}

func TestDurableDatabaseFileSystemTruncate(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mockDatabase := test.MockDatabase(app)

		storage.PageLoggerCompactInterval = 0
		defer func() {
			storage.PageLoggerCompactInterval = storage.DefaultPageLoggerCompactInterval
		}()

		dfs := storage.NewDurableDatabaseFileSystem(
			app.Cluster.LocalFS(),
			app.Cluster.LocalFS(),
			app.DatabaseManager.PageLogManager().Get(mockDatabase.DatabaseID, mockDatabase.BranchID, app.Cluster.LocalFS()),
			config.StorageModeLocal,
			mockDatabase.DatabaseID,
			mockDatabase.BranchID,
			4096,
		)

		timestamp := time.Now().UTC().UnixNano()

		for i := range storage.RangeMaxPages * 4 {
			_, err := dfs.WriteAt(timestamp, timestamp, make([]byte, 4096), int64(i*4096))

			if err != nil {
				t.Error("expected nil, got", err)
			}
		}

		// Check the file size
		size, err := dfs.Size()

		if err != nil {
			t.Error("expected nil, got", err)
		}

		if size != (storage.RangeMaxPages*4)*4096 {
			t.Errorf("expected %d bytes, got %d", (storage.RangeMaxPages*4)*4096, size)
		}

		// Need to compact here to flush pages from the page log to the ranges
		err = dfs.Compact()

		if err != nil {
			t.Fatal("expected nil, got", err)
		}

		// Truncate the file to 10MB, but we do not grow ranges
		err = dfs.Truncate(10 * 1024 * 1024)

		if err != nil {
			t.Error("expected nil, got", err)
		}

		// Check the file size
		size, err = dfs.Size()

		if err != nil {
			t.Error("expected nil, got", err)
		}

		if size != 10*1024*1024 {
			t.Errorf("expected %d bytes, got %d", 10*1024*1024, size)
		}

		err = dfs.Truncate(0)

		if err != nil {
			t.Error("expected nil, got", err)
		}

		// Check the file size
		size, err = dfs.Size()

		if err != nil {
			t.Error("expected nil, got", err)
		}

		if size != 0 {
			t.Error("expected 0, got", size)
		}

	})
}

func TestDurableDatabaseFileSystemWriteAt(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mockDatabase := test.MockDatabase(app)

		dfs := storage.NewDurableDatabaseFileSystem(
			app.Cluster.LocalFS(),
			app.Cluster.LocalFS(),
			app.DatabaseManager.PageLogManager().Get(mockDatabase.DatabaseID, mockDatabase.BranchID, app.Cluster.LocalFS()),
			config.StorageModeLocal,
			mockDatabase.DatabaseID,
			mockDatabase.BranchID,
			4096,
		)

		data := make([]byte, 4096)
		rand.Read(data)

		// Write some data to the file
		n, err := dfs.WriteAt(0, 0, data, 0)

		if err != nil {
			t.Error("expected nil, got", err)
		}

		if n != 4096 {
			t.Error("expected 4096, got", n)
		}

		// Read the data back
		buffer := make([]byte, 4096)

		n, err = dfs.ReadAt(0, 0, buffer, 0, 4096)

		if err != nil {
			t.Error("expected nil, got", err)
		}

		if n != 4096 {
			t.Error("expected 4096, got", n)
		}

		if !bytes.Equal(buffer[:n], data) {
			t.Error("expected test, got", string(buffer[:n]))
		}
	})
}

func TestDurableDatabaseFileSystemWithoutWriteHook(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mockDatabase := test.MockDatabase(app)

		var hookCalled bool

		dfs := storage.NewDurableDatabaseFileSystem(
			app.Cluster.LocalFS(),
			app.Cluster.LocalFS(),
			app.DatabaseManager.PageLogManager().Get(mockDatabase.DatabaseID, mockDatabase.BranchID, app.Cluster.LocalFS()),
			config.StorageModeLocal,
			mockDatabase.DatabaseID,
			mockDatabase.BranchID,
			4096,
		).SetWriteHook(func(o int64, d []byte) {
			hookCalled = true
		})

		if dfs == nil {
			t.Error("expected a database file system, got nil")
		}

		data := make([]byte, 4096)
		rand.Read(data)

		n, err := dfs.WriteWithoutWriteHook(func() (int, error) {
			return dfs.WriteAt(0, 0, data, 0)
		})

		if err != nil {
			t.Error("expected nil, got", err)
		}

		if n != 4096 {
			t.Error("expected 4096, got", n)
		}

		if hookCalled {
			t.Error("expected hook not to be called")
		}
	})
}
