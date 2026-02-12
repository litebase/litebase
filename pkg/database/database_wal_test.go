package database_test

import (
	"bytes"
	"crypto/rand"
	"io"
	"testing"
	"time"

	"slices"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/database"
	"github.com/litebase/litebase/pkg/server"
)

func TestNewDatabaseWAL(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		walManager, _ := app.DatabaseManager.Resources(mock.Branch).DatabaseWALManager()

		databaseWAL := database.NewDatabaseWAL(
			app.Cluster.Node(),
			app.DatabaseManager.ConnectionManager(),
			app.Cluster.MemoryManager,
			mock.DatabaseID,
			mock.DatabaseBranchID,
			app.Cluster.LocalFS(),
			walManager,
			time.Now().UTC().UnixNano(),
		)

		if databaseWAL.DatabaseID != mock.DatabaseID {
			t.Fatalf("Expected DatabaseID to be %s, got %s", mock.DatabaseID, databaseWAL.DatabaseID)
		}

		if databaseWAL.BranchID != mock.DatabaseBranchID {
			t.Fatalf("Expected BranchID to be %s, got %s", mock.DatabaseBranchID, databaseWAL.BranchID)
		}
	})
}

func TestDatabaseWAL_Checkpointing(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		walManager, _ := app.DatabaseManager.Resources(
			mock.Branch,
		).DatabaseWALManager()

		databaseWAL := database.NewDatabaseWAL(
			app.Cluster.Node(),
			app.DatabaseManager.ConnectionManager(),
			app.Cluster.MemoryManager,
			mock.DatabaseID,
			mock.DatabaseBranchID,
			app.Cluster.LocalFS(),
			walManager,
			time.Now().UTC().UnixNano(),
		)

		if databaseWAL.Checkpointing() {
			t.Fatal("Expected Checkpointing to be false")
		}
	})
}

func TestDatabaseWAL_Close(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		walManager, _ := app.DatabaseManager.Resources(
			mock.Branch,
		).DatabaseWALManager()

		databaseWAL := database.NewDatabaseWAL(
			app.Cluster.Node(),
			app.DatabaseManager.ConnectionManager(),
			app.Cluster.MemoryManager,
			mock.DatabaseID,
			mock.DatabaseBranchID,
			app.Cluster.LocalFS(),
			walManager,
			time.Now().UTC().UnixNano(),
		)

		err := databaseWAL.Close()
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}
	})
}
func TestDatabaseWAL_File(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		walManager, _ := app.DatabaseManager.Resources(
			mock.Branch,
		).DatabaseWALManager()

		databaseWAL := database.NewDatabaseWAL(
			app.Cluster.Node(),
			app.DatabaseManager.ConnectionManager(),
			app.Cluster.MemoryManager,
			mock.DatabaseID,
			mock.DatabaseBranchID,
			app.Cluster.LocalFS(),
			walManager,
			time.Now().UTC().UnixNano(),
		)

		file, err := databaseWAL.File()

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if file == nil {
			t.Fatal("Expected file to be created")
		}
	})
}

func TestDatabaseWAL_Hash(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		walManager, _ := app.DatabaseManager.Resources(
			mock.Branch,
		).DatabaseWALManager()

		databaseWAL := database.NewDatabaseWAL(
			app.Cluster.Node(),
			app.DatabaseManager.ConnectionManager(),
			app.Cluster.MemoryManager,
			mock.DatabaseID,
			mock.DatabaseBranchID,
			app.Cluster.LocalFS(),
			walManager,
			time.Now().UTC().UnixNano(),
		)

		hash := databaseWAL.Hash()

		if hash == "" {
			t.Fatal("Expected hash to be created")
		}
	})
}

func TestDatabaseWAL_IsCheckpointed(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		walManager, _ := app.DatabaseManager.Resources(
			mock.Branch,
		).DatabaseWALManager()

		databaseWAL := database.NewDatabaseWAL(
			app.Cluster.Node(),
			app.DatabaseManager.ConnectionManager(),
			app.Cluster.MemoryManager,
			mock.DatabaseID,
			mock.DatabaseBranchID,
			app.Cluster.LocalFS(),
			walManager,
			time.Now().UTC().UnixNano(),
		)

		if databaseWAL.IsCheckpointed() {
			t.Fatal("Expected IsCheckpointed to be false")
		}
	})
}

func TestDatabaseWAL_MarkCheckpointed(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		walManager, _ := app.DatabaseManager.Resources(
			mock.Branch,
		).DatabaseWALManager()

		databaseWAL := database.NewDatabaseWAL(
			app.Cluster.Node(),
			app.DatabaseManager.ConnectionManager(),
			app.Cluster.MemoryManager,
			mock.DatabaseID,
			mock.DatabaseBranchID,
			app.Cluster.LocalFS(),
			walManager,
			time.Now().UTC().UnixNano(),
		)

		databaseWAL.MarkCheckpointed()

		if !databaseWAL.IsCheckpointed() {
			t.Fatal("Expected IsCheckpointed to be true")
		}
	})
}

func TestDatabaseWAL_ReadAt(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		walManager, _ := app.DatabaseManager.Resources(
			mock.Branch,
		).DatabaseWALManager()

		databaseWAL := database.NewDatabaseWAL(
			app.Cluster.Node(),
			app.DatabaseManager.ConnectionManager(),
			app.Cluster.MemoryManager,
			mock.DatabaseID,
			mock.DatabaseBranchID,
			app.Cluster.LocalFS(),
			walManager,
			time.Now().UTC().UnixNano(),
		)

		data := make([]byte, 4096)

		if _, err := rand.Read(data); err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		file, err := databaseWAL.File()

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if _, err := file.WriteAt(data, 0); err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		readData := make([]byte, 4096)

		_, err = databaseWAL.ReadAt("", readData, 0)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if len(readData) != 4096 {
			t.Fatalf("Expected data length to be 10, got %d", len(readData))
		}

		if !bytes.Equal(data, readData) {
			t.Fatal("Expected data to be equal")
		}
	})
}

func TestDatabaseWAL_RequiresCheckpoint(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		walManager, _ := app.DatabaseManager.Resources(
			mock.Branch,
		).DatabaseWALManager()

		databaseWAL := database.NewDatabaseWAL(
			app.Cluster.Node(),
			app.DatabaseManager.ConnectionManager(),
			app.Cluster.MemoryManager,
			mock.DatabaseID,
			mock.DatabaseBranchID,
			app.Cluster.LocalFS(),
			walManager,
			time.Now().UTC().UnixNano(),
		)

		if databaseWAL.RequiresCheckpoint() {
			t.Fatal("Expected RequiresCheckpoint to be false")
		}

		if _, err := databaseWAL.WriteAt("", []byte("test"), 0); err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if !databaseWAL.RequiresCheckpoint() {
			t.Fatal("Expected RequiresCheckpoint to be true")
		}
	})
}

func TestDatabaseWAL_SetCheckpointing(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		walManager, _ := app.DatabaseManager.Resources(
			mock.Branch,
		).DatabaseWALManager()

		databaseWAL := database.NewDatabaseWAL(
			app.Cluster.Node(),
			app.DatabaseManager.ConnectionManager(),
			app.Cluster.MemoryManager,
			mock.DatabaseID,
			mock.DatabaseBranchID,
			app.Cluster.LocalFS(),
			walManager,
			time.Now().UTC().UnixNano(),
		)

		if err := databaseWAL.SetCheckpointing(true); err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if !databaseWAL.Checkpointing() {
			t.Fatal("Expected Checkpointing to be true")
		}
	})
}

func TestDatabaseWAL_Size(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		walManager, _ := app.DatabaseManager.Resources(
			mock.Branch,
		).DatabaseWALManager()

		databaseWAL := database.NewDatabaseWAL(
			app.Cluster.Node(),
			app.DatabaseManager.ConnectionManager(),
			app.Cluster.MemoryManager,
			mock.DatabaseID,
			mock.DatabaseBranchID,
			app.Cluster.LocalFS(),
			walManager,
			time.Now().UTC().UnixNano(),
		)

		size, _ := databaseWAL.Size()

		if size != 0 {
			t.Fatal("Expected Size to be 0")
		}

		_, err := databaseWAL.WriteAt("", make([]byte, 4096), 0)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if err := databaseWAL.Sync(); err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		size, _ = databaseWAL.Size()

		if size != 4096 {
			t.Fatalf("Expected Size to be 4096, got %d", size)
		}
	})
}

func TestDatabaseWAL_Sync(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		walManager, _ := app.DatabaseManager.Resources(
			mock.Branch,
		).DatabaseWALManager()

		databaseWAL := database.NewDatabaseWAL(
			app.Cluster.Node(),
			app.DatabaseManager.ConnectionManager(),
			app.Cluster.MemoryManager,
			mock.DatabaseID,
			mock.DatabaseBranchID,
			app.Cluster.LocalFS(),
			walManager,
			time.Now().UTC().UnixNano(),
		)

		err := databaseWAL.Sync()

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}
	})
}

func TestDatabaseWAL_Timestamp(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		walManager, _ := app.DatabaseManager.Resources(
			mock.Branch,
		).DatabaseWALManager()

		databaseWAL := database.NewDatabaseWAL(
			app.Cluster.Node(),
			app.DatabaseManager.ConnectionManager(),
			app.Cluster.MemoryManager,
			mock.DatabaseID,
			mock.DatabaseBranchID,
			app.Cluster.LocalFS(),
			walManager,
			time.Now().UTC().UnixNano(),
		)

		timestamp := databaseWAL.Timestamp()

		if timestamp == 0 {
			t.Fatal("Expected timestamp to be created")
		}
	})
}

func TestDatabaseWAL_Truncate(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		walManager, _ := app.DatabaseManager.Resources(
			mock.Branch,
		).DatabaseWALManager()

		databaseWAL := database.NewDatabaseWAL(
			app.Cluster.Node(),
			app.DatabaseManager.ConnectionManager(),
			app.Cluster.MemoryManager,
			mock.DatabaseID,
			mock.DatabaseBranchID,
			app.Cluster.LocalFS(),
			walManager,
			time.Now().UTC().UnixNano(),
		)

		err := databaseWAL.Truncate(0)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		size, _ := databaseWAL.Size()

		if size != 0 {
			t.Fatalf("Expected Size to be 0, got %d", size)
		}
	})
}

func TestDatabaseWAL_WriteAt(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		walManager, _ := app.DatabaseManager.Resources(
			mock.Branch,
		).DatabaseWALManager()

		databaseWAL := database.NewDatabaseWAL(
			app.Cluster.Node(),
			app.DatabaseManager.ConnectionManager(),
			app.Cluster.MemoryManager,
			mock.DatabaseID,
			mock.DatabaseBranchID,
			app.Cluster.LocalFS(),
			walManager,
			time.Now().UTC().UnixNano(),
		)

		data := make([]byte, 4096)

		if _, err := rand.Read(data); err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		n, err := databaseWAL.WriteAt("", data, 0)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if n != 4096 {
			t.Fatalf("Expected n to be 4096, got %d", n)
		}
	})
}

func TestDatabaseWAL_ReadAfterWrite(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		walManager, _ := app.DatabaseManager.Resources(
			mock.Branch,
		).DatabaseWALManager()

		databaseWAL := database.NewDatabaseWAL(
			app.Cluster.Node(),
			app.DatabaseManager.ConnectionManager(),
			app.Cluster.MemoryManager,
			mock.DatabaseID,
			mock.DatabaseBranchID,
			app.Cluster.LocalFS(),
			walManager,
			time.Now().UTC().UnixNano(),
		)

		numberOfWrites := 1000
		sizeOfWrite := 1000
		writes := make([][]byte, 0)
		writeBytes := make([]byte, sizeOfWrite)
		readBytes := make([]byte, sizeOfWrite)

		for i := range numberOfWrites {
			if _, err := rand.Read(writeBytes); err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}

			writes = append(writes, slices.Clone(writeBytes))
			n, err := databaseWAL.WriteAt("", writeBytes, int64(i*sizeOfWrite))

			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}

			if n != sizeOfWrite {
				t.Fatalf("Expected n to be %d, got %d", sizeOfWrite, n)
			}

			_, err = databaseWAL.ReadAt("", readBytes, int64(i*sizeOfWrite))

			if err != nil {
				t.Fatalf("Expected no error, for iteration %d got %v", i, err)
			}

			if !bytes.Equal(writes[i], readBytes) {
				t.Fatalf("Expected data to be equal, got %v", err)
			}

			if i%100 == 0 {
				time.Sleep(10 * time.Millisecond)
			}
		}

		err := databaseWAL.Sync()

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		file, err := databaseWAL.File()

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		fileSize, err := databaseWAL.Size()

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if fileSize != int64(numberOfWrites*sizeOfWrite) {
			t.Fatalf("Expected file size to be %d, got %d", numberOfWrites*sizeOfWrite, fileSize)
		}

		_, err = file.Seek(0, io.SeekStart)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		for i := range numberOfWrites {
			n, err := file.Read(readBytes)

			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}

			if n != sizeOfWrite {
				t.Fatalf("Expected n to be %d, got %d", sizeOfWrite, n)
			}

			if !bytes.Equal(writes[i], readBytes) {
				t.Logf("Expected %v got %v", writes[i], readBytes)
				t.Fatalf("Expected data to be equal for iteration %d", i)
			}
		}
	})
}

func TestDatabaseWAL_HeavyWrite(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		walManager, _ := app.DatabaseManager.Resources(
			mock.Branch,
		).DatabaseWALManager()

		databaseWAL := database.NewDatabaseWAL(
			app.Cluster.Node(),
			app.DatabaseManager.ConnectionManager(),
			app.Cluster.MemoryManager,
			mock.DatabaseID,
			mock.DatabaseBranchID,
			app.Cluster.LocalFS(),
			walManager,
			time.Now().UTC().UnixNano(),
		)

		numberOfWrites := 10000
		sizeOfWrite := 4096
		writes := make([][]byte, 0)
		writeBytes := make([]byte, sizeOfWrite)
		readBytes := make([]byte, sizeOfWrite)

		for i := range numberOfWrites {
			if _, err := rand.Read(writeBytes); err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}

			writes = append(writes, slices.Clone(writeBytes))
			n, err := databaseWAL.WriteAt("", writeBytes, int64(i*sizeOfWrite))

			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}

			if n != sizeOfWrite {
				t.Fatalf("Expected n to be %d, got %d", sizeOfWrite, n)
			}

			if i%100 == 0 {
				time.Sleep(10 * time.Millisecond)
			}
		}

		err := databaseWAL.Sync()

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		for i := range numberOfWrites {
			n, err := databaseWAL.ReadAt("", readBytes, int64(i*sizeOfWrite))

			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}

			if n != sizeOfWrite {
				t.Fatalf("Expected n to be %d, got %d", sizeOfWrite, n)
			}

			if !bytes.Equal(writes[i], readBytes) {
				t.Fatalf("Expected data to be equal for iteration %d", i)
			}
		}
	})
}

func TestDatabaseWAL_ImplicitAndExplicitTransactions(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		walManager, _ := app.DatabaseManager.Resources(
			mock.Branch,
		).DatabaseWALManager()

		databaseWAL := database.NewDatabaseWAL(
			app.Cluster.Node(),
			app.DatabaseManager.ConnectionManager(),
			app.Cluster.MemoryManager,
			mock.DatabaseID,
			mock.DatabaseBranchID,
			app.Cluster.LocalFS(),
			walManager,
			time.Now().UTC().UnixNano(),
		)

		// Simulate implicit transaction (autocommit) on connection A
		// These writes go directly to WAL without calling Begin()
		connA := "connection-A"
		implicitData := []byte("implicit-transaction-data-from-connection-A")
		implicitOffset := int64(0)

		// Write without Begin() - simulates autocommit mode
		n, err := databaseWAL.WriteAt(connA, implicitData, implicitOffset)

		if err != nil {
			t.Fatalf("Implicit write failed: %v", err)
		}

		if n != len(implicitData) {
			t.Fatalf("Expected to write %d bytes, got %d", len(implicitData), n)
		}

		// Verify implicit write went directly to WAL (not buffered)
		readBuf := make([]byte, len(implicitData))
		n, err = databaseWAL.ReadAt(connA, readBuf, implicitOffset)

		if err != nil {
			t.Fatalf("Failed to read implicit write: %v", err)
		}

		if !bytes.Equal(implicitData, readBuf) {
			t.Fatal("Implicit write data mismatch")
		}

		// Now simulate explicit transaction on connection B
		connB := "connection-B"
		err = databaseWAL.Begin(connB)

		if err != nil {
			t.Fatalf("Failed to begin explicit transaction: %v", err)
		}

		// Write to transaction buffer
		explicitData := []byte("explicit-transaction-data-from-connection-B-with-buffer")
		explicitOffset := int64(1024)

		n, err = databaseWAL.WriteAt(connB, explicitData, explicitOffset)

		if err != nil {
			t.Fatalf("Explicit write failed: %v", err)
		}

		if n != len(explicitData) {
			t.Fatalf("Expected to write %d bytes, got %d", len(explicitData), n)
		}

		// Connection B should be able to read its buffered write
		readBuf = make([]byte, len(explicitData))
		n, err = databaseWAL.ReadAt(connB, readBuf, explicitOffset)

		if err != nil {
			t.Fatalf("Failed to read buffered write: %v", err)
		}

		if !bytes.Equal(explicitData, readBuf) {
			t.Fatal("Buffered write data mismatch")
		}

		// Connection A should NOT see connection B's buffered write
		readBuf = make([]byte, len(explicitData))
		n, err = databaseWAL.ReadAt(connA, readBuf, explicitOffset)

		// This should return an error or EOF since the data isn't flushed yet
		if err == nil && bytes.Equal(explicitData, readBuf) {
			t.Fatal("Connection A should not see buffered data from connection B")
		}

		// Connection A can still read its own implicit write
		readBuf = make([]byte, len(implicitData))
		n, err = databaseWAL.ReadAt(connA, readBuf, implicitOffset)

		if err != nil {
			t.Fatalf("Failed to read implicit write after explicit transaction started: %v", err)
		}

		if !bytes.Equal(implicitData, readBuf) {
			t.Fatal("Implicit write data should still be readable")
		}

		// End explicit transaction - this flushes the buffer
		err = databaseWAL.End(connB)

		if err != nil {
			t.Fatalf("Failed to end explicit transaction: %v", err)
		}

		// After flushing, connection A should now see connection B's data
		readBuf = make([]byte, len(explicitData))
		n, err = databaseWAL.ReadAt(connA, readBuf, explicitOffset)

		if err != nil {
			t.Fatalf("Failed to read flushed data: %v", err)
		}

		if !bytes.Equal(explicitData, readBuf) {
			t.Fatal("Flushed data mismatch")
		}

		// Verify both writes are persisted
		if err := databaseWAL.Sync(); err != nil {
			t.Fatalf("Failed to sync WAL: %v", err)
		}

		// Read both writes from file
		file, err := databaseWAL.File()

		if err != nil {
			t.Fatalf("Failed to get WAL file: %v", err)
		}

		// Read implicit write from file
		readBuf = make([]byte, len(implicitData))
		n, err = file.ReadAt(readBuf, implicitOffset)

		if err != nil {
			t.Fatalf("Failed to read implicit data from file: %v", err)
		}

		if !bytes.Equal(implicitData, readBuf) {
			t.Fatal("Implicit data not persisted correctly")
		}

		// Read explicit write from file
		readBuf = make([]byte, len(explicitData))
		n, err = file.ReadAt(readBuf, explicitOffset)

		if err != nil {
			t.Fatalf("Failed to read explicit data from file: %v", err)
		}

		if !bytes.Equal(explicitData, readBuf) {
			t.Fatal("Explicit data not persisted correctly")
		}
	})
}

func TestDatabaseWAL_MultipleConnectionsWithBuffering(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		walManager, _ := app.DatabaseManager.Resources(
			mock.Branch,
		).DatabaseWALManager()

		databaseWAL := database.NewDatabaseWAL(
			app.Cluster.Node(),
			app.DatabaseManager.ConnectionManager(),
			app.Cluster.MemoryManager,
			mock.DatabaseID,
			mock.DatabaseBranchID,
			app.Cluster.LocalFS(),
			walManager,
			time.Now().UTC().UnixNano(),
		)

		// Test that only one connection can have an active transaction at a time
		conn1 := "connection-1"
		conn2 := "connection-2"

		// Connection 1 starts transaction
		err := databaseWAL.Begin(conn1)

		if err != nil {
			t.Fatalf("Failed to begin transaction on connection 1: %v", err)
		}

		// Connection 2 attempts to start transaction - should fail
		err = databaseWAL.Begin(conn2)

		if err == nil {
			t.Fatal("Expected error when starting second transaction, got nil")
		}

		if err.Error() != "transaction already in progress" {
			t.Fatalf("Expected 'transaction already in progress' error, got: %v", err)
		}

		// Connection 1 writes to buffer
		data1 := []byte("connection-1-transaction-data")
		offset1 := int64(0)

		n, err := databaseWAL.WriteAt(conn1, data1, offset1)

		if err != nil {
			t.Fatalf("Failed to write from connection 1: %v", err)
		}

		if n != len(data1) {
			t.Fatalf("Expected to write %d bytes, got %d", len(data1), n)
		}

		// Connection 2 attempts to write without Begin() - should write directly to file
		data2 := []byte("connection-2-direct-write-no-transaction")
		offset2 := int64(1024)

		n, err = databaseWAL.WriteAt(conn2, data2, offset2)

		if err != nil {
			t.Fatalf("Failed to write from connection 2: %v", err)
		}

		if n != len(data2) {
			t.Fatalf("Expected to write %d bytes, got %d", len(data2), n)
		}

		// Connection 1 should see its own buffered data
		readBuf := make([]byte, len(data1))
		n, err = databaseWAL.ReadAt(conn1, readBuf, offset1)

		if err != nil {
			t.Fatalf("Failed to read from connection 1: %v", err)
		}

		if !bytes.Equal(data1, readBuf) {
			t.Fatal("Connection 1 data mismatch")
		}

		// Connection 2 should see its own direct write
		readBuf = make([]byte, len(data2))
		n, err = databaseWAL.ReadAt(conn2, readBuf, offset2)

		if err != nil {
			t.Fatalf("Failed to read from connection 2: %v", err)
		}

		if !bytes.Equal(data2, readBuf) {
			t.Fatal("Connection 2 data mismatch")
		}

		// Connection 1 ends transaction
		err = databaseWAL.End(conn1)

		if err != nil {
			t.Fatalf("Failed to end transaction on connection 1: %v", err)
		}

		// Now connection 2 should be able to start a transaction
		err = databaseWAL.Begin(conn2)

		if err != nil {
			t.Fatalf("Failed to begin transaction on connection 2 after connection 1 ended: %v", err)
		}

		// Connection 2 writes to buffer
		data3 := []byte("connection-2-transaction-data")
		offset3 := int64(2048)

		n, err = databaseWAL.WriteAt(conn2, data3, offset3)

		if err != nil {
			t.Fatalf("Failed to write from connection 2 transaction: %v", err)
		}

		// Connection 2 should see its buffered data
		readBuf = make([]byte, len(data3))
		n, err = databaseWAL.ReadAt(conn2, readBuf, offset3)

		if err != nil {
			t.Fatalf("Failed to read from connection 2 buffer: %v", err)
		}

		if !bytes.Equal(data3, readBuf) {
			t.Fatal("Connection 2 buffered data mismatch")
		}

		// End connection 2 transaction
		err = databaseWAL.End(conn2)

		if err != nil {
			t.Fatalf("Failed to end transaction on connection 2: %v", err)
		}

		// Verify all data is persisted
		if err := databaseWAL.Sync(); err != nil {
			t.Fatalf("Failed to sync WAL: %v", err)
		}

		// Verify all three writes
		allWrites := []struct {
			data   []byte
			offset int64
		}{
			{data1, offset1},
			{data2, offset2},
			{data3, offset3},
		}

		for i, write := range allWrites {
			readBuf = make([]byte, len(write.data))
			n, err = databaseWAL.ReadAt("verify", readBuf, write.offset)

			if err != nil {
				t.Fatalf("Failed to read write %d: %v", i, err)
			}

			if !bytes.Equal(write.data, readBuf) {
				t.Fatalf("Write %d data mismatch", i)
			}
		}
	})
}
