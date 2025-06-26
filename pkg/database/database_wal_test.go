package database_test

import (
	"bytes"
	"crypto/rand"
	"io"
	"log"
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

		walManager, _ := app.DatabaseManager.Resources(
			mock.DatabaseID,
			mock.BranchID,
		).DatabaseWALManager()

		databaseWAL := database.NewDatabaseWAL(
			app.Cluster.Node(),
			app.DatabaseManager.ConnectionManager(),
			mock.DatabaseID,
			mock.BranchID,
			app.Cluster.LocalFS(),
			walManager,
			time.Now().UTC().UnixNano(),
		)

		if databaseWAL == nil {
			t.Fatal("Expected databaseWAL to be created")
		}

		if databaseWAL.DatabaseID != mock.DatabaseID {
			t.Fatalf("Expected DatabaseID to be %s, got %s", mock.DatabaseID, databaseWAL.DatabaseID)
		}

		if databaseWAL.BranchID != mock.BranchID {
			t.Fatalf("Expected BranchID to be %s, got %s", mock.BranchID, databaseWAL.BranchID)
		}
	})
}

func TestDatabaseWAL_Checkpointing(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		walManager, _ := app.DatabaseManager.Resources(
			mock.DatabaseID,
			mock.BranchID,
		).DatabaseWALManager()

		databaseWAL := database.NewDatabaseWAL(
			app.Cluster.Node(),
			app.DatabaseManager.ConnectionManager(),
			mock.DatabaseID,
			mock.BranchID,
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
			mock.DatabaseID,
			mock.BranchID,
		).DatabaseWALManager()

		databaseWAL := database.NewDatabaseWAL(
			app.Cluster.Node(),
			app.DatabaseManager.ConnectionManager(),
			mock.DatabaseID,
			mock.BranchID,
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
			mock.DatabaseID,
			mock.BranchID,
		).DatabaseWALManager()

		databaseWAL := database.NewDatabaseWAL(
			app.Cluster.Node(),
			app.DatabaseManager.ConnectionManager(),
			mock.DatabaseID,
			mock.BranchID,
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
			mock.DatabaseID,
			mock.BranchID,
		).DatabaseWALManager()

		databaseWAL := database.NewDatabaseWAL(
			app.Cluster.Node(),
			app.DatabaseManager.ConnectionManager(),
			mock.DatabaseID,
			mock.BranchID,
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
			mock.DatabaseID,
			mock.BranchID,
		).DatabaseWALManager()

		databaseWAL := database.NewDatabaseWAL(
			app.Cluster.Node(),
			app.DatabaseManager.ConnectionManager(),
			mock.DatabaseID,
			mock.BranchID,
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
			mock.DatabaseID,
			mock.BranchID,
		).DatabaseWALManager()

		databaseWAL := database.NewDatabaseWAL(
			app.Cluster.Node(),
			app.DatabaseManager.ConnectionManager(),
			mock.DatabaseID,
			mock.BranchID,
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
			mock.DatabaseID,
			mock.BranchID,
		).DatabaseWALManager()

		databaseWAL := database.NewDatabaseWAL(
			app.Cluster.Node(),
			app.DatabaseManager.ConnectionManager(),
			mock.DatabaseID,
			mock.BranchID,
			app.Cluster.LocalFS(),
			walManager,
			time.Now().UTC().UnixNano(),
		)

		data := make([]byte, 4096)
		rand.Read(data)

		file, _ := databaseWAL.File()

		file.WriteAt(data, 0)

		readData := make([]byte, 4096)

		_, err := databaseWAL.ReadAt(readData, 0)

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
			mock.DatabaseID,
			mock.BranchID,
		).DatabaseWALManager()

		databaseWAL := database.NewDatabaseWAL(
			app.Cluster.Node(),
			app.DatabaseManager.ConnectionManager(),
			mock.DatabaseID,
			mock.BranchID,
			app.Cluster.LocalFS(),
			walManager,
			time.Now().UTC().UnixNano(),
		)

		if databaseWAL.RequiresCheckpoint() {
			t.Fatal("Expected RequiresCheckpoint to be false")
		}

		databaseWAL.WriteAt([]byte("test"), 0)

		if !databaseWAL.RequiresCheckpoint() {
			t.Fatal("Expected RequiresCheckpoint to be true")
		}
	})
}

func TestDatabaseWAL_SetCheckpointing(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		walManager, _ := app.DatabaseManager.Resources(
			mock.DatabaseID,
			mock.BranchID,
		).DatabaseWALManager()

		databaseWAL := database.NewDatabaseWAL(
			app.Cluster.Node(),
			app.DatabaseManager.ConnectionManager(),
			mock.DatabaseID,
			mock.BranchID,
			app.Cluster.LocalFS(),
			walManager,
			time.Now().UTC().UnixNano(),
		)

		databaseWAL.SetCheckpointing(true)

		if !databaseWAL.Checkpointing() {
			t.Fatal("Expected Checkpointing to be true")
		}
	})
}

func TestDatabaseWAL_Size(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		walManager, _ := app.DatabaseManager.Resources(
			mock.DatabaseID,
			mock.BranchID,
		).DatabaseWALManager()

		databaseWAL := database.NewDatabaseWAL(
			app.Cluster.Node(),
			app.DatabaseManager.ConnectionManager(),
			mock.DatabaseID,
			mock.BranchID,
			app.Cluster.LocalFS(),
			walManager,
			time.Now().UTC().UnixNano(),
		)

		size, _ := databaseWAL.Size()

		if size != 0 {
			t.Fatal("Expected Size to be 0")
		}

		_, err := databaseWAL.WriteAt(make([]byte, 4096), 0)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		databaseWAL.Sync()

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
			mock.DatabaseID,
			mock.BranchID,
		).DatabaseWALManager()

		databaseWAL := database.NewDatabaseWAL(
			app.Cluster.Node(),
			app.DatabaseManager.ConnectionManager(),
			mock.DatabaseID,
			mock.BranchID,
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
			mock.DatabaseID,
			mock.BranchID,
		).DatabaseWALManager()

		databaseWAL := database.NewDatabaseWAL(
			app.Cluster.Node(),
			app.DatabaseManager.ConnectionManager(),
			mock.DatabaseID,
			mock.BranchID,
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
			mock.DatabaseID,
			mock.BranchID,
		).DatabaseWALManager()

		databaseWAL := database.NewDatabaseWAL(
			app.Cluster.Node(),
			app.DatabaseManager.ConnectionManager(),
			mock.DatabaseID,
			mock.BranchID,
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
			mock.DatabaseID,
			mock.BranchID,
		).DatabaseWALManager()

		databaseWAL := database.NewDatabaseWAL(
			app.Cluster.Node(),
			app.DatabaseManager.ConnectionManager(),
			mock.DatabaseID,
			mock.BranchID,
			app.Cluster.LocalFS(),
			walManager,
			time.Now().UTC().UnixNano(),
		)

		data := make([]byte, 4096)
		rand.Read(data)

		n, err := databaseWAL.WriteAt(data, 0)

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
			mock.DatabaseID,
			mock.BranchID,
		).DatabaseWALManager()

		databaseWAL := database.NewDatabaseWAL(
			app.Cluster.Node(),
			app.DatabaseManager.ConnectionManager(),
			mock.DatabaseID,
			mock.BranchID,
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
			rand.Read(writeBytes)
			writes = append(writes, slices.Clone(writeBytes))
			n, err := databaseWAL.WriteAt(writeBytes, int64(i*sizeOfWrite))

			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}

			if n != sizeOfWrite {
				t.Fatalf("Expected n to be %d, got %d", sizeOfWrite, n)
			}

			_, err = databaseWAL.ReadAt(readBytes, int64(i*sizeOfWrite))

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

		file.Seek(0, io.SeekStart)

		for i := range numberOfWrites {
			n, err := file.Read(readBytes)

			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}

			if n != sizeOfWrite {
				t.Fatalf("Expected n to be %d, got %d", sizeOfWrite, n)
			}

			if !bytes.Equal(writes[i], readBytes) {
				log.Println("Expected", writes[i], "got", readBytes)
				t.Fatalf("Expected data to be equal for iteration %d", i)
			}
		}
	})
}

func TestDatabaseWAL_HeavyWrite(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		walManager, _ := app.DatabaseManager.Resources(
			mock.DatabaseID,
			mock.BranchID,
		).DatabaseWALManager()

		databaseWAL := database.NewDatabaseWAL(
			app.Cluster.Node(),
			app.DatabaseManager.ConnectionManager(),
			mock.DatabaseID,
			mock.BranchID,
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
			rand.Read(writeBytes)
			writes = append(writes, slices.Clone(writeBytes))
			n, err := databaseWAL.WriteAt(writeBytes, int64(i*sizeOfWrite))

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
			n, err := databaseWAL.ReadAt(readBytes, int64(i*sizeOfWrite))

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
