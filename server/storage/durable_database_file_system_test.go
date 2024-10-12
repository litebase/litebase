package storage_test

import (
	"litebase/internal/config"
	"litebase/internal/test"
	"litebase/server"
	"litebase/server/storage"
	"testing"
)

func TestNewDurableDatabaseFileSystem(t *testing.T) {
	test.Run(t, func(app *server.App) {
		mockDatabase := test.MockDatabase(app)

		dfs := storage.NewDurableDatabaseFileSystem(
			storage.LocalFS(),
			config.STORAGE_MODE_LOCAL,
			mockDatabase.DatabaseId,
			mockDatabase.BranchId,
			4096,
		)

		if dfs == nil {
			t.Error("expected local database file system, got nil")
		}
	})
}

func TestDurableDatabaseFileSystemFileSystem(t *testing.T) {
	test.Run(t, func(app *server.App) {
		mockDatabase := test.MockDatabase(app)

		dfs := storage.NewDurableDatabaseFileSystem(
			storage.LocalFS(),
			config.STORAGE_MODE_LOCAL,
			mockDatabase.DatabaseId,
			mockDatabase.BranchId,
			4096,
		)

		fs := dfs.FileSystem()

		if fs == nil {
			t.Error("expected file system, got nil")
		}

		if fs != storage.LocalFS() {
			t.Error("expected local file system, got", fs)
		}
	})
}

func TestDurableDatabaseFileSystemGetRangeFile(t *testing.T) {
	test.Run(t, func(app *server.App) {
		mockDatabase := test.MockDatabase(app)

		dfs := storage.NewDurableDatabaseFileSystem(
			storage.LocalFS(),
			config.STORAGE_MODE_LOCAL,
			mockDatabase.DatabaseId,
			mockDatabase.BranchId,
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

func TestDurableDatabaseFileSystemMetadata(t *testing.T) {
	test.Run(t, func(app *server.App) {
		mockDatabase := test.MockDatabase(app)

		dfs := storage.NewDurableDatabaseFileSystem(
			storage.LocalFS(),
			config.STORAGE_MODE_LOCAL,
			mockDatabase.DatabaseId,
			mockDatabase.BranchId,
			4096,
		)

		metadata := dfs.Metadata()

		if metadata == nil {
			t.Error("expected metadata, got nil")
		}
	})
}
func TestDurableDatabaseFileSystemOpen(t *testing.T) {
	test.Run(t, func(app *server.App) {
		mockDatabase := test.MockDatabase(app)

		dfs := storage.NewDurableDatabaseFileSystem(
			storage.LocalFS(),
			config.STORAGE_MODE_LOCAL,
			mockDatabase.DatabaseId,
			mockDatabase.BranchId,
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

func TestDurableDatabaseFileSystemPageSize(t *testing.T) {
	test.Run(t, func(app *server.App) {
		mockDatabase := test.MockDatabase(app)

		dfs := storage.NewDurableDatabaseFileSystem(
			storage.LocalFS(),
			config.STORAGE_MODE_LOCAL,
			mockDatabase.DatabaseId,
			mockDatabase.BranchId,
			4096,
		)

		pageSize := dfs.PageSize()

		if pageSize != 4096 {
			t.Error("expected 4096, got", pageSize)
		}
	})
}

func TestDurableDatabaseFileSystemPath(t *testing.T) {
	test.Run(t, func(app *server.App) {
		mockDatabase := test.MockDatabase(app)

		dfs := storage.NewDurableDatabaseFileSystem(
			storage.LocalFS(),
			config.STORAGE_MODE_LOCAL,
			mockDatabase.DatabaseId,
			mockDatabase.BranchId,
			4096,
		)

		path := dfs.Path()

		if path != config.STORAGE_MODE_LOCAL {
			t.Error("expected local, got", path)
		}
	})
}

func TestDurableDatabaseFileSystemReadAt(t *testing.T) {
	test.Run(t, func(app *server.App) {
		mockDatabase := test.MockDatabase(app)

		dfs := storage.NewDurableDatabaseFileSystem(
			storage.LocalFS(),
			config.STORAGE_MODE_LOCAL,
			mockDatabase.DatabaseId,
			mockDatabase.BranchId,
			4096,
		)

		buffer := make([]byte, 4096)

		n, err := dfs.ReadAt(buffer, 0, 4096)

		if err != nil {
			t.Error("expected nil, got", err)
		}

		if n != 0 {
			t.Error("expected 0, got", n)
		}

		// Write some data to the file
		n, err = dfs.WriteAt([]byte("test"), 0)

		if err != nil {
			t.Error("expected nil, got", err)
		}

		if n != 4 {
			t.Error("expected 4, got", n)
		}

		// Read the data back
		n, err = dfs.ReadAt(buffer, 0, 4)

		if err != nil {
			t.Error("expected nil, got", err)
		}

		if n != 4 {
			t.Error("expected 4, got", n)
		}

		if string(buffer[:n]) != "test" {
			t.Error("expected test, got", string(buffer[:n]))
		}
	})
}

func TestDurableDatabaseFileSystemSetWriteHook(t *testing.T) {
	test.Run(t, func(app *server.App) {
		mockDatabase := test.MockDatabase(app)

		var offset int64
		var data []byte

		dfs := storage.NewDurableDatabaseFileSystem(
			storage.LocalFS(),
			config.STORAGE_MODE_LOCAL,
			mockDatabase.DatabaseId,
			mockDatabase.BranchId,
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

func TestDurableDatabaseFileSystemSize(t *testing.T) {
	test.Run(t, func(app *server.App) {
		mockDatabase := test.MockDatabase(app)

		dfs := storage.NewDurableDatabaseFileSystem(
			storage.LocalFS(),
			config.STORAGE_MODE_LOCAL,
			mockDatabase.DatabaseId,
			mockDatabase.BranchId,
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
	test.Run(t, func(app *server.App) {
		mockDatabase := test.MockDatabase(app)

		dfs := storage.NewDurableDatabaseFileSystem(
			storage.LocalFS(),
			config.STORAGE_MODE_LOCAL,
			mockDatabase.DatabaseId,
			mockDatabase.BranchId,
			4096,
		)
		err := dfs.Shutdown()

		if err != nil {
			t.Error("expected nil, got", err)
		}
	})
}

func TestDurableDatabaseFileSystemTruncate(t *testing.T) {
	test.Run(t, func(app *server.App) {
		mockDatabase := test.MockDatabase(app)

		dfs := storage.NewDurableDatabaseFileSystem(
			storage.LocalFS(),
			config.STORAGE_MODE_LOCAL,
			mockDatabase.DatabaseId,
			mockDatabase.BranchId,
			4096,
		)

		// Write 2048 pages of data
		for i := 0; i < 2048; i++ {
			_, err := dfs.WriteAt(make([]byte, 4096), int64(i*4096))

			if err != nil {
				t.Error("expected nil, got", err)
			}
		}

		// Check the file size
		size, err := dfs.Size()

		if err != nil {
			t.Error("expected nil, got", err)
		}

		if size != 2048*4096 {
			t.Error("expected 2048 pages, got", size)
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

		// Truncate the file to 10MB, but we do not grow data ranges
		err = dfs.Truncate(10 * 1024 * 1024)

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
	test.Run(t, func(app *server.App) {
		mockDatabase := test.MockDatabase(app)

		dfs := storage.NewDurableDatabaseFileSystem(
			storage.LocalFS(),
			config.STORAGE_MODE_LOCAL,
			mockDatabase.DatabaseId,
			mockDatabase.BranchId,
			4096,
		)

		// Write some data to the file
		n, err := dfs.WriteAt([]byte("test"), 0)

		if err != nil {
			t.Error("expected nil, got", err)
		}

		if n != 4 {
			t.Error("expected 4, got", n)
		}

		// Read the data back
		buffer := make([]byte, 4)

		n, err = dfs.ReadAt(buffer, 0, 4)

		if err != nil {
			t.Error("expected nil, got", err)
		}

		if n != 4 {
			t.Error("expected 4, got", n)
		}

		if string(buffer[:n]) != "test" {
			t.Error("expected test, got", string(buffer[:n]))
		}
	})
}

func TestDurableDatabaseFileSystemWithoutWriteHook(t *testing.T) {
	test.Run(t, func(app *server.App) {
		mockDatabase := test.MockDatabase(app)

		var hookCalled bool

		dfs := storage.NewDurableDatabaseFileSystem(
			storage.LocalFS(),
			config.STORAGE_MODE_LOCAL,
			mockDatabase.DatabaseId,
			mockDatabase.BranchId,
			4096,
		).SetWriteHook(func(o int64, d []byte) {
			hookCalled = true
		})

		if dfs == nil {
			t.Error("expected a database file system, got nil")
		}

		n, err := dfs.WriteWithoutWriteHook(func() (int, error) {
			return dfs.WriteAt([]byte("test"), 0)
		})

		if err != nil {
			t.Error("expected nil, got", err)
		}

		if n != 4 {
			t.Error("expected 4, got", n)
		}

		if hookCalled {
			t.Error("expected hook not to be called")
		}
	})
}
