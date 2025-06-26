package storage_test

import (
	"bytes"
	"crypto/rand"
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/config"
	"github.com/litebase/litebase/pkg/server"
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

		_, err := dfs.WriteAt(int64(1), data, 0)

		if err != nil {
			t.Error("expected nil, got", err)
		}

		rangeFile, err := dfs.GetRangeFile(1)

		if err != nil {
			t.Error("expected nil, got", err)
		}

		size, err := rangeFile.Size()

		if err != nil {
			t.Error("expected nil, got", err)
		}

		if size != 0 {
			t.Error("expected 0, got", size)
		}

		err = dfs.Compact()

		if err != nil {
			t.Error("expected nil, got", err)
		}

		size, err = rangeFile.Size()

		if err != nil {
			t.Error("expected nil, got", err)
		}

		if size != 4096 {
			t.Error("expected 4096, got", size)
		}
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

		n, err := dfs.ReadAt(int64(0), buffer, 0, 4096)

		if err != nil {
			t.Error("expected nil, got", err)
		}

		if n != 0 {
			t.Error("expected 0, got", n)
		}

		data := make([]byte, 4096)

		rand.Read(data)

		// Write some data to the file
		n, err = dfs.WriteAt(int64(0), data, 0)

		if err != nil {
			t.Error("expected nil, got", err)
		}

		if n != 4096 {
			t.Error("expected 4096, got", n)
		}

		// Read the data back
		n, err = dfs.ReadAt(int64(0), buffer, 0, 4096)

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
			_, err := dfs.WriteAt(timestamp, make([]byte, 4096), int64(i*4096))

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
		n, err := dfs.WriteAt(int64(0), data, 0)

		if err != nil {
			t.Error("expected nil, got", err)
		}

		if n != 4096 {
			t.Error("expected 4096, got", n)
		}

		// Read the data back
		buffer := make([]byte, 4096)

		n, err = dfs.ReadAt(int64(0), buffer, 0, 4096)

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
			return dfs.WriteAt(int64(0), data, 0)
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
