package storage_test

import (
	"context"
	"io"
	"os"
	"testing"
	"time"

	internalStorage "github.com/litebase/litebase/internal/storage"
	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/server"
	"github.com/litebase/litebase/server/storage"
)

func TestNewTieredFileSystemDriver(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		tieredFileSystemDriver := storage.NewTieredFileSystemDriver(
			context.Background(),
			storage.NewLocalFileSystemDriver(app.Config.DataPath+"/local"),
			storage.NewLocalFileSystemDriver(app.Config.DataPath+"/object"),
		)

		if tieredFileSystemDriver == nil {
			t.Error("NewTieredFileSystemDriver returned nil")
		}
	})
}

func TestTieredFileSystem_ClearFiles(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		tieredFileSystemDriver := storage.NewTieredFileSystemDriver(
			context.Background(),
			storage.NewLocalFileSystemDriver(app.Config.DataPath+"/local"),
			storage.NewLocalFileSystemDriver(app.Config.DataPath+"/object"),
		)

		err := os.WriteFile(app.Config.DataPath+"/local/test_1", []byte("test"), 0644)

		if err != nil {
			t.Error(err)
		}

		err = tieredFileSystemDriver.ClearFiles()

		if err != nil {
			t.Error(err)
		}

		if _, err := os.Stat(app.Config.DataPath + "/local/test_1"); !os.IsNotExist(err) {
			t.Fatal("TieredFileSystem.ClearFiles did not remove the file")
		}

		if len(tieredFileSystemDriver.Files) != 0 {
			t.Errorf("TieredFileSystem.ClearFiles returned incorrect number of files, got %d", len(tieredFileSystemDriver.Files))
		}

		// Create a new file
		err = tieredFileSystemDriver.WriteFile("test_2", []byte("test"), 0644)

		if err != nil {
			t.Error(err)
		}

		// Clear the files again
		err = tieredFileSystemDriver.ClearFiles()

		if err != nil {
			t.Error(err)
		}

		if len(tieredFileSystemDriver.Files) != 0 {
			t.Errorf("TieredFileSystem.ClearFiles returned incorrect number of files, got %d", len(tieredFileSystemDriver.Files))
		}

		// Create a new file
		err = tieredFileSystemDriver.WriteFile("test_3", []byte("test"), 0644)

		if err != nil {
			t.Error(err)
		}

		// Clear the files again
		err = tieredFileSystemDriver.ClearFiles()

		if err != nil {
			t.Error(err)
		}

		if len(tieredFileSystemDriver.Files) != 0 {
			t.Errorf("TieredFileSystem.ClearFiles returned incorrect number of files, got %d", len(tieredFileSystemDriver.Files))
		}
	})
}

func TestTieredFileSystemDriver_Create(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		tieredFileSystemDriver := storage.NewTieredFileSystemDriver(
			context.Background(),
			storage.NewLocalFileSystemDriver(app.Config.DataPath+"/local"),
			storage.NewLocalFileSystemDriver(app.Config.DataPath+"/object"),
		)

		tieredFile, err := tieredFileSystemDriver.Create("test")

		if err != nil {
			t.Error(err)
		}

		if tieredFile == nil {
			t.Error("TieredFileSystemDriver.Create returned nil")
		}

		// A new file should exist in local storage
		info, err := tieredFileSystemDriver.Stat("test")

		if err != nil {
			t.Error(err)
		}

		if info == nil {
			t.Error("TieredFileSystemDriver.Stat returned nil")
		}
	})
}

func TestTieredFileSystemDriver_Files(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		tieredFileSystemDriver := storage.NewTieredFileSystemDriver(
			context.Background(),
			storage.NewLocalFileSystemDriver(app.Config.DataPath+"/local"),
			storage.NewLocalFileSystemDriver(app.Config.DataPath+"/object"),
		)

		tieredFile, err := tieredFileSystemDriver.Create("test")

		if err != nil {
			t.Error(err)
		}

		if tieredFile == nil {
			t.Error("TieredFileSystemDriver.Create returned nil")
		}

		files := tieredFileSystemDriver.Files

		if len(files) != 1 {
			t.Errorf("TieredFileSystemDriver.Files returned incorrect number of files, got %d", len(files))
		}
	})
}

func TestTieredFileSystemDriver_Mkdir(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		tieredFileSystemDriver := storage.NewTieredFileSystemDriver(
			context.Background(),
			storage.NewLocalFileSystemDriver(app.Config.DataPath+"/local"),
			storage.NewLocalFileSystemDriver(app.Config.DataPath+"/object"),
		)

		err := tieredFileSystemDriver.Mkdir("test/", 0755)

		if err != nil {
			t.Error(err)
		}

		// A new file should exist in local storage
		info, err := tieredFileSystemDriver.Stat("test/")

		if err != nil && !os.IsNotExist(err) {
			t.Error(err)
		}

		if info == nil {
			t.Error("TieredFileSystemDriver.Stat returned nil")
		}
	})
}

func TestTieredFileSystemDriver_MkdirAll(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		tieredFileSystemDriver := storage.NewTieredFileSystemDriver(
			context.Background(),
			storage.NewLocalFileSystemDriver(app.Config.DataPath+"/local"),
			storage.NewLocalFileSystemDriver(app.Config.DataPath+"/object"),
		)

		err := tieredFileSystemDriver.MkdirAll("test/test/test/", 0755)

		if err != nil {
			t.Error(err)
		}

		// A new file should exist in local storage
		info, err := tieredFileSystemDriver.Stat("test/test/test/")

		if err != nil && !os.IsNotExist(err) {
			t.Error(err)
		}

		if info == nil {
			t.Error("TieredFileSystemDriver.Stat returned nil")
		}
	})
}

func TestTieredFileSystemDriver_Open(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		tieredFileSystemDriver := storage.NewTieredFileSystemDriver(
			context.Background(),
			storage.NewLocalFileSystemDriver(app.Config.DataPath+"/local"),
			storage.NewLocalFileSystemDriver(app.Config.DataPath+"/object"),
		)

		_, err := tieredFileSystemDriver.Open("test")

		if err == nil || !os.IsNotExist(err) {
			t.Errorf("TieredFileSystemDriver.Open should return os.IsNotExist error, got %v", err)
		}

		tieredFile, err := tieredFileSystemDriver.Create("test")

		if err != nil {
			t.Error(err)
		}

		if tieredFile == nil {
			t.Error("TieredFileSystemDriver.Create returned nil")
		}

		tieredFile.Close()

		tieredFile, err = tieredFileSystemDriver.Open("test")

		if err != nil {
			t.Error(err)
		}

		if tieredFile == nil {
			t.Error("TieredFileSystemDriver.Open returned nil")
		}

		tieredFile.Close()
	})
}

func TestTieredFileSystemDriver_OpenDurableFile(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		lfsd := storage.NewLocalFileSystemDriver(app.Config.DataPath + "/local")
		dfsd := storage.NewLocalFileSystemDriver(app.Config.DataPath + "/object")

		tieredFileSystemDriver := storage.NewTieredFileSystemDriver(
			context.Background(),
			lfsd,
			dfsd,
		)

		// If the file is not found in local storage or durable storage, the
		// file system driver should return an os.IsNotExist error.
		_, err := tieredFileSystemDriver.Open("test.txt")

		if err == nil || !os.IsNotExist(err) {
			t.Errorf("TieredFileSystemDriver.Open should return os.IsNotExist error, got %v", err)
		}

		// When a file is not found on local storage, the file system driver
		// should attempt to find the file in durable storage.
		err = dfsd.WriteFile("test.txt", []byte("test"), 0644)

		if err != nil {
			t.Error(err)
		}

		tieredFile, err := tieredFileSystemDriver.Open("test.txt")

		if err != nil {
			t.Error(err)
		}

		if tieredFile == nil {
			t.Error("TieredFileSystemDriver.Open returned nil")
		}

		tieredFile.Seek(0, io.SeekStart)
		data := make([]byte, 4)
		_, err = tieredFile.Read(data)

		if err != nil {
			t.Error(err)
		}

		tieredFile.Seek(0, io.SeekStart)

		// Ensure the file contents are correct
		if string(data) != "test" {
			t.Errorf("TieredFileSystemDriver.Open returned incorrect data, got %s", data)
		}

		// Verify that the file was copied to local storage
		_, err = lfsd.Stat("test.txt")

		if err != nil {
			t.Error(err)
		}

		tieredFile.Close()
	})
}

func TestTieredFileSystemDriver_OpenFile(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		tieredFileSystemDriver := storage.NewTieredFileSystemDriver(
			context.Background(),
			storage.NewLocalFileSystemDriver(app.Config.DataPath+"/local"),
			storage.NewLocalFileSystemDriver(app.Config.DataPath+"/object"),
		)

		// Test open read only file that does not exist
		_, err := tieredFileSystemDriver.OpenFile("test", os.O_RDONLY, 0644)

		if err == nil || !os.IsNotExist(err) {
			t.Errorf("TieredFileSystemDriver.OpenFile should return os.IsNotExist error, got %v", err)
		}

		tieredFile, err := tieredFileSystemDriver.Create("test")

		if err != nil {
			t.Error(err)
		}

		if tieredFile == nil {
			t.Error("TieredFileSystemDriver.Create returned nil")
		}

		tieredFile.Close()

		tieredFile, err = tieredFileSystemDriver.OpenFile("test", os.O_RDONLY, 0644)

		if err != nil {
			t.Error(err)
		}

		if tieredFile == nil {
			t.Error("TieredFileSystemDriver.OpenFile returned nil")
		}

		// Attempting to write to a read only file should return an error
		_, err = tieredFile.Write([]byte("test"))

		if err == nil {
			t.Error("TieredFileSystemDriver.Write should return an error")
		}

		if tieredFile != nil {
			tieredFile.Close()
		}

		// Create a new file
		_, err = tieredFileSystemDriver.Create("test2")

		if err != nil {
			t.Error(err)
		}

		// Test opening a write only file
		tieredFile, err = tieredFileSystemDriver.OpenFile("test2", os.O_WRONLY, 0644)

		if err != nil {
			t.Error(err)
		}

		if tieredFile == nil {
			t.Fatal("TieredFileSystemDriver.OpenFile returned nil")
		}

		// Should be able to write to the file
		n, err := tieredFile.Write([]byte("test"))

		if err != nil {
			t.Error(err)
		}

		if n != 4 {
			t.Errorf("TieredFileSystemDriver.Write returned incorrect number of bytes, got %d", n)
		}

		// Create a new file
		_, err = tieredFileSystemDriver.Create("test3")

		if err != nil {
			t.Error(err)
		}

		// Test opening a read write file
		tieredFile, err = tieredFileSystemDriver.OpenFile("test3", os.O_RDWR, 0644)

		if err != nil {
			t.Error(err)
		}

		if tieredFile == nil {
			t.Error("TieredFileSystemDriver.OpenFile returned nil")
		}

		// Should be able to read and write to the file
		tieredFile.Write([]byte("test"))

		tieredFile.Seek(0, io.SeekStart)

		data := make([]byte, 4)

		_, err = tieredFile.Read(data)

		if err != nil {
			t.Error(err)
		}

		// Test opening a file with create flag
		tieredFile, err = tieredFileSystemDriver.OpenFile("test4", os.O_CREATE, 0644)

		if err != nil {
			t.Error(err)
		}

		if tieredFile == nil {
			t.Error("TieredFileSystemDriver.OpenFile returned nil")
		}

		// Test opening a file with create flag and read write flag
		tieredFile, err = tieredFileSystemDriver.OpenFile("test4", os.O_CREATE|os.O_RDWR, 0644)

		if err != nil {
			t.Error(err)
		}

		if tieredFile == nil {
			t.Error("TieredFileSystemDriver.OpenFile returned nil")
		}

		// Write to the file
		n, err = tieredFile.Write([]byte("test"))

		if err != nil {
			t.Error(err)
		}

		if n != 4 {
			t.Errorf("TieredFileSystemDriver.Write returned incorrect number of bytes, got %d", n)
		}
	})
}

func TestTieredFileSystemDriver_ReadDir(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		tieredFileSystemDriver := storage.NewTieredFileSystemDriver(
			context.Background(),
			storage.NewLocalFileSystemDriver(app.Config.DataPath+"/local"),
			storage.NewLocalFileSystemDriver(app.Config.DataPath+"/object"),
		)

		_, err := tieredFileSystemDriver.ReadDir("dir/")

		if err == nil || !os.IsNotExist(err) {
			t.Errorf("TieredFileSystemDriver.ReadDir should return os.IsNotExist error, got %v", err)
		}

		err = tieredFileSystemDriver.Mkdir("dir/", 0755)

		if err != nil {
			t.Error(err)
		}

		tieredFile, err := tieredFileSystemDriver.Create("dir/test")

		if err != nil {
			t.Error(err)
		}

		if tieredFile == nil {
			t.Error("TieredFileSystemDriver.Create returned nil")
		}

		files, err := tieredFileSystemDriver.ReadDir("dir/")

		if err != nil {
			t.Error(err)
		}

		if len(files) != 1 {
			t.Errorf("TieredFileSystemDriver.ReadDir returned incorrect number of files, got %d", len(files))
		}
	})
}

func TestTieredFileSystemDriver_ReadFile(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		dfsd := storage.NewLocalFileSystemDriver(app.Config.DataPath + "/object")

		tieredFileSystemDriver := storage.NewTieredFileSystemDriver(
			context.Background(),
			storage.NewLocalFileSystemDriver(app.Config.DataPath+"/local"),
			dfsd,
		)

		_, err := tieredFileSystemDriver.ReadFile("test")

		if err == nil || !os.IsNotExist(err) {
			t.Errorf("TieredFileSystemDriver.ReadFile should return os.IsNotExist error, got %v", err)
		}

		err = dfsd.WriteFile("test", []byte("test"), 0644)

		if err != nil {
			t.Error(err)
		}

		// Read from the durable file system
		data, err := tieredFileSystemDriver.ReadFile("test")

		if err != nil {
			t.Error(err)
		}

		if string(data) != "test" {
			t.Errorf("TieredFileSystemDriver.ReadFile returned incorrect data, got %s", data)
		}

		// Read from the local file system
		data, err = tieredFileSystemDriver.ReadFile("test")

		if err != nil {
			t.Error(err)
		}

		if string(data) != "test" {
			t.Errorf("TieredFileSystemDriver.ReadFile returned incorrect data, got %s", data)
		}
	})
}

func TestTieredFileSystemDriver_ReleaseOldestFile(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		dfsd := storage.NewLocalFileSystemDriver(app.Config.DataPath + "/object")

		tieredFileSystemDriver := storage.NewTieredFileSystemDriver(
			context.Background(),
			storage.NewLocalFileSystemDriver(app.Config.DataPath+"/local"),
			dfsd,
		)

		err := tieredFileSystemDriver.ReleaseOldestFile()

		if err == nil || err != storage.ErrNoTieredFilesToRemove {
			t.Errorf("TieredFileSystemDriver.ReleaseOldestFile should return storage.ErrNoTieredFilesToRemove error, got %v", err)
		}

		if tieredFileSystemDriver.FileCount != 0 {
			t.Errorf("TieredFileSystemDriver.FileCount should be 0, got %d", tieredFileSystemDriver.FileCount)
		}

		tieredFile1, _ := tieredFileSystemDriver.OpenFile("test1", os.O_RDWR|os.O_CREATE, 0644)

		_, err = tieredFile1.Write([]byte("test"))

		if err != nil {
			t.Error(err)
		}

		err = tieredFileSystemDriver.ReleaseOldestFile()

		if err != storage.ErrNoTieredFilesToRemove {
			t.Fatalf("TieredFileSystemDriver.ReleaseOldestFile should return storage.ErrNoTieredFilesToRemove error, got %v", err)
		}

		tieredFileSystemDriver.OpenFile("test2", os.O_RDWR|os.O_CREATE, 0644)

		if tieredFileSystemDriver.FileCount != 2 {
			t.Errorf("TieredFileSystemDriver.FileCount should be 2, got %d", tieredFileSystemDriver.FileCount)
		}

		err = tieredFileSystemDriver.Flush()

		if err != nil {
			t.Fatal(err)
		}

		err = tieredFileSystemDriver.ReleaseOldestFile()

		if err != nil {
			t.Errorf("TieredFileSystemDriver.ReleaseOldestFile should return nil, got %v", err)
		}

		if tieredFileSystemDriver.FileCount != 1 {
			t.Errorf("TieredFileSystemDriver.FileCount should be 1, got %d", tieredFileSystemDriver.FileCount)
		}

		err = tieredFileSystemDriver.ReleaseOldestFile()

		if err != nil {
			t.Errorf("TieredFileSystemDriver.ReleaseOldestFile should return nil, got %v", err)
		}

		if tieredFileSystemDriver.FileCount != 0 {
			t.Errorf("TieredFileSystemDriver.FileCount should be 0, got %d", tieredFileSystemDriver.FileCount)
		}
	})
}

func TestTieredFileSystemDriver_ReleaseOldestFile_WhileReading(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		dfsd := storage.NewLocalFileSystemDriver(app.Config.DataPath + "/object")

		tieredFileSystemDriver := storage.NewTieredFileSystemDriver(
			context.Background(),
			storage.NewLocalFileSystemDriver(app.Config.DataPath+"/local"),
			dfsd,
		)

		tieredFile1, _ := tieredFileSystemDriver.OpenFile("test1", os.O_RDWR|os.O_CREATE, 0644)

		tieredFile1.Write([]byte("helloworld"))

		data := make([]byte, 5)

		tieredFile1.Seek(0, io.SeekStart)

		_, err := tieredFile1.Read(data)

		if err != nil {
			t.Error(err)
		}

		if string(data) != "hello" {
			t.Errorf("TieredFileSystemDriver.Read returned incorrect data, got %s", data)
		}

		err = tieredFileSystemDriver.ReleaseOldestFile()

		if err == nil {
			t.Errorf("TieredFileSystemDriver.ReleaseOldestFile should return nil, got %v", err)
		}

		tieredFileSystemDriver.Flush()

		err = tieredFileSystemDriver.ReleaseOldestFile()

		if err != nil {
			t.Errorf("TieredFileSystemDriver.ReleaseOldestFile should return nil, got %v", err)
		}

		if tieredFileSystemDriver.FileCount != 0 {
			t.Errorf("TieredFileSystemDriver.FileCount should be 0, got %d", tieredFileSystemDriver.FileCount)
		}

		tieredFile1.Seek(5, io.SeekStart)

		_, err = tieredFile1.Read(data)

		if err != nil {
			t.Error(err)
		}

		if string(data) != "world" {
			t.Errorf("TieredFileSystemDriver.Read returned incorrect data, got %s", data)
		}
	})
}

func TestTieredFileSystemDriver_Remove(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		dfsd := storage.NewLocalFileSystemDriver(app.Config.DataPath + "/object")

		tieredFileSystemDriver := storage.NewTieredFileSystemDriver(
			context.Background(),
			storage.NewLocalFileSystemDriver(app.Config.DataPath+"/local"),
			dfsd,
		)

		err := tieredFileSystemDriver.Remove("test")

		if err == nil || !os.IsNotExist(err) {
			t.Errorf("TieredFileSystemDriver.Remove should return os.IsNotExist error, got %v", err)
		}

		err = dfsd.WriteFile("test", []byte("test"), 0644)

		if err != nil {
			t.Error(err)
		}

		err = tieredFileSystemDriver.Remove("test")

		if err != nil {
			t.Error(err)
		}

		_, err = tieredFileSystemDriver.ReadFile("test")

		if err == nil || !os.IsNotExist(err) {
			t.Errorf("TieredFileSystemDriver.ReadFile should return os.IsNotExist error, got %v", err)
		}
	})
}

func TestTieredFileSystemDriver_RemoveAll(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		dfsd := storage.NewLocalFileSystemDriver(app.Config.DataPath + "/object")

		tieredFileSystemDriver := storage.NewTieredFileSystemDriver(
			context.Background(),
			storage.NewLocalFileSystemDriver(app.Config.DataPath+"/local"),
			dfsd,
		)

		err := tieredFileSystemDriver.RemoveAll("dir/")

		if err != nil {
			t.Errorf("TieredFileSystemDriver.RemoveAll should return nil, got %v", err)
		}

		err = tieredFileSystemDriver.MkdirAll("dir/dir2/dir3", 0755)

		if err != nil {
			t.Error(err)
		}

		tieredFile, err := tieredFileSystemDriver.Create("dir/test")

		if err != nil {
			t.Error(err)
		}

		if tieredFile == nil {
			t.Error("TieredFileSystemDriver.Create returned nil")
		}

		tieredFile, err = tieredFileSystemDriver.Create("dir/dir2/test")

		if err != nil {
			t.Error(err)
		}

		if tieredFile == nil {
			t.Error("TieredFileSystemDriver.Create returned nil")
		}

		tieredFile, err = tieredFileSystemDriver.Create("dir/dir2/dir3/test")

		if err != nil {
			t.Error(err)
		}

		if tieredFile == nil {
			t.Error("TieredFileSystemDriver.Create returned nil")
		}

		err = tieredFileSystemDriver.RemoveAll("dir/")

		if err != nil {
			t.Error(err)
		}

		_, err = tieredFileSystemDriver.ReadFile("dir/test")

		if err == nil || !os.IsNotExist(err) {
			t.Errorf("TieredFileSystemDriver.ReadFile should return os.IsNotExist error, got %v", err)
		}

		_, err = tieredFileSystemDriver.ReadFile("dir/dir2/test")

		if err == nil || !os.IsNotExist(err) {
			t.Errorf("TieredFileSystemDriver.ReadFile should return os.IsNotExist error, got %v", err)
		}

		_, err = tieredFileSystemDriver.ReadFile("dir/dir2/dir3/test")

		if err == nil || !os.IsNotExist(err) {
			t.Errorf("TieredFileSystemDriver.ReadFile should return os.IsNotExist error, got %v", err)
		}
	})
}

func TestTieredFileSystemDriver_Rename(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		dfsd := storage.NewLocalFileSystemDriver(app.Config.DataPath + "/object")

		tieredFileSystemDriver := storage.NewTieredFileSystemDriver(
			context.Background(),
			storage.NewLocalFileSystemDriver(app.Config.DataPath+"/local"),
			dfsd,
		)

		err := tieredFileSystemDriver.Rename("test", "'test2")

		if err == nil || !os.IsNotExist(err) {
			t.Errorf("TieredFileSystemDriver.Rename should return os.IsNotExist error, got %v", err)
		}

		err = dfsd.WriteFile("test.txt", []byte("test"), 0644)

		if err != nil {
			t.Error(err)
		}

		err = tieredFileSystemDriver.Rename("test.txt", "test2.txt")

		if err != nil {
			t.Error(err)
		}

		_, err = tieredFileSystemDriver.ReadFile("test.txt")

		if err == nil || !os.IsNotExist(err) {
			t.Errorf("TieredFileSystemDriver.ReadFile should return os.IsNotExist error, got %v", err)
		}

		data, err := tieredFileSystemDriver.ReadFile("test2.txt")

		if err != nil {
			t.Error(err)
		}

		if string(data) != "test" {
			t.Errorf("TieredFileSystemDriver.ReadFile returned incorrect data, got %s", data)
		}
	})
}

func TestTieredFileSystemDriver_Stat(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		dfsd := storage.NewLocalFileSystemDriver(app.Config.DataPath + "/object")

		tieredFileSystemDriver := storage.NewTieredFileSystemDriver(
			context.Background(),
			storage.NewLocalFileSystemDriver(app.Config.DataPath+"/local"),
			dfsd,
		)

		_, err := tieredFileSystemDriver.Stat("test")

		if err == nil || !os.IsNotExist(err) {
			t.Errorf("TieredFileSystemDriver.Stat should return os.IsNotExist error, got %v", err)
		}

		err = dfsd.WriteFile("test.txt", []byte("test"), 0644)

		if err != nil {
			t.Error(err)
		}

		info, err := tieredFileSystemDriver.Stat("test.txt")

		if err != nil {
			t.Error(err)
		}

		if info == nil {
			t.Error("TieredFileSystemDriver.Stat returned nil")
		}

		if info.Name() != "test.txt" {
			t.Errorf("TieredFileSystemDriver.Stat returned incorrect name, got %s", info.Name())
		}

		if info.Size() != 4 {
			t.Errorf("TieredFileSystemDriver.Stat returned incorrect size, got %d", info.Size())
		}
	})
}

func TestTieredFileSystemDriver_Truncate(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		dfsd := storage.NewLocalFileSystemDriver(app.Config.DataPath + "/object")

		tieredFileSystemDriver := storage.NewTieredFileSystemDriver(
			context.Background(),
			storage.NewLocalFileSystemDriver(app.Config.DataPath+"/local"),
			dfsd,
		)

		err := tieredFileSystemDriver.Truncate("test", 4)

		if err == nil || !os.IsNotExist(err) {
			t.Errorf("TieredFileSystemDriver.Truncate should return os.IsNotExist error, got %v", err)
		}

		err = dfsd.WriteFile("test.txt", []byte("test"), 0644)

		if err != nil {
			t.Error(err)
		}

		err = tieredFileSystemDriver.Truncate("test.txt", 2)

		if err != nil {
			t.Error(err)
		}

		info, err := tieredFileSystemDriver.Stat("test.txt")

		if err != nil {
			t.Error(err)
		}

		if info.Size() != 2 {
			t.Errorf("TieredFileSystemDriver.Truncate returned incorrect size, got %d", info.Size())
		}
	})
}

func TestTieredFileSystemDriver_WriteFile(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		dfsd := storage.NewLocalFileSystemDriver(app.Config.DataPath + "/object")

		tieredFileSystemDriver := storage.NewTieredFileSystemDriver(
			context.Background(),
			storage.NewLocalFileSystemDriver(app.Config.DataPath+"/local"),
			dfsd,
		)

		err := tieredFileSystemDriver.WriteFile("test.txt", []byte("test"), 0644)

		if err != nil {
			t.Error(err)
		}

		data, err := tieredFileSystemDriver.ReadFile("test.txt")

		if err != nil {
			t.Error(err)
		}

		if string(data) != "test" {
			t.Errorf("TieredFileSystemDriver.WriteFile returned incorrect data, got %s", data)
		}
	})
}

func TestTieredFileIsReleasedWhenTTLHasPassed(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		tieredFileSystemDriver := storage.NewTieredFileSystemDriver(
			context.Background(),
			storage.NewLocalFileSystemDriver(app.Config.DataPath+"/local"),
			storage.NewLocalFileSystemDriver(app.Config.DataPath+"/object"),
		)

		now := time.Now()

		file1 := &storage.TieredFile{
			TieredFileSystemDriver: tieredFileSystemDriver,
			Flag:                   os.O_RDWR,
			Closed:                 false,
			UpdatedAt:              now.Add(-time.Minute * 30),
			CreatedAt:              now.Add(-time.Hour * 2),
		}

		tests := []struct {
			name     string
			files    map[string]*storage.TieredFile
			path     string
			expected *storage.TieredFile
			ok       bool
		}{
			{
				name: "File exists and is not closed or stale",
				files: map[string]*storage.TieredFile{
					"file1": file1,
				},
				path:     "file1",
				expected: file1,
				ok:       true,
			},
			{
				name: "File exists but is closed",
				files: map[string]*storage.TieredFile{
					"file2": {TieredFileSystemDriver: tieredFileSystemDriver, Closed: true, UpdatedAt: now.Add(-time.Minute * 30), CreatedAt: now.Add(-time.Hour * 2)},
				},
				path:     "file2",
				expected: nil,
				ok:       false,
			},
			{
				name: "File exists but is stale",
				files: map[string]*storage.TieredFile{
					"file3": {TieredFileSystemDriver: tieredFileSystemDriver, Closed: false, UpdatedAt: now.Add(-time.Hour * 25), CreatedAt: now.Add(-time.Hour * 26)},
				},
				path:     "file3",
				expected: nil,
				ok:       false,
			},
			{
				name:     "File does not exist",
				files:    map[string]*storage.TieredFile{},
				path:     "file4",
				expected: nil,
				ok:       false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				tieredFileSystemDriver.Files = tt.files

				file, ok := tieredFileSystemDriver.GetTieredFile(tt.path)

				if (file == nil && tt.expected != nil) || (file != nil && tt.expected == nil) || (file != nil && tt.expected != nil && (*file != *tt.expected)) || ok != tt.ok {
					t.Errorf("expected (%v, %v), got (%v, %v)", tt.expected, tt.ok, file, ok)
				}
			})
		}
	})
}

func TestTieredFileIsFlushedToDurableStorageAfterUpdate(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		dfsd := storage.NewLocalFileSystemDriver(app.Config.DataPath + "/object")

		tieredFileSystemDriver := storage.NewTieredFileSystemDriver(
			context.Background(),
			storage.NewLocalFileSystemDriver(app.Config.DataPath+"/local"),
			dfsd,
			func(context context.Context, tieredFileSystemDriver *storage.TieredFileSystemDriver) {
				tieredFileSystemDriver.WriteInterval = time.Millisecond * 1
			},
		)

		tieredFile, err := tieredFileSystemDriver.Create("test.txt")

		if err != nil {
			t.Error(err)
		}

		if tieredFile == nil {
			t.Error("TieredFileSystemDriver.Create returned nil")
		}

		tieredFile.Write([]byte("test"))

		time.Sleep(time.Millisecond * 2)

		data, err := dfsd.ReadFile("test.txt")

		if err != nil {
			t.Error(err)
		}

		if string(data) != "test" {
			t.Errorf("TieredFileSystemDriver.WriteFile returned incorrect data, got %s", data)
		}
	})
}

func TestTieredFileSystemDriverLocalFileWithDifferentAccessFlags(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		dfsd := storage.NewLocalFileSystemDriver(app.Config.DataPath + "/object")

		tieredFileSystemDriver := storage.NewTieredFileSystemDriver(
			context.Background(),
			storage.NewLocalFileSystemDriver(app.Config.DataPath+"/local"),
			dfsd,
		)

		// Test open read only file that does not exist
		_, err := tieredFileSystemDriver.OpenFile("test", os.O_RDONLY, 0644)

		if err == nil || !os.IsNotExist(err) {
			t.Errorf("TieredFileSystemDriver.OpenFile should return os.IsNotExist error, got %v", err)
		}

		tieredFile, err := tieredFileSystemDriver.Create("test")

		if err != nil {
			t.Error(err)
		}

		if tieredFile == nil {
			t.Error("TieredFileSystemDriver.Create returned nil")
		}

		tieredFile.Close()

		tieredFile, err = tieredFileSystemDriver.OpenFile("test", os.O_RDONLY, 0644)

		if err != nil {
			t.Error(err)
		}

		if tieredFile == nil {
			t.Error("TieredFileSystemDriver.OpenFile returned nil")
		}

		// Attempting to write to a read only file should return an error
		_, err = tieredFile.Write([]byte("test"))

		if err == nil {
			t.Error("TieredFileSystemDriver.Write should return an error")
		}

		if tieredFile != nil {
			tieredFile.Close()
		}

		// Create a new file
		_, err = tieredFileSystemDriver.Create("test2")

		if err != nil {
			t.Error(err)
		}

		// Test opening a write only file
		tieredFile, err = tieredFileSystemDriver.OpenFile("test2", os.O_WRONLY, 0644)

		if err != nil {
			t.Error(err)
		}

		if tieredFile == nil {
			t.Error("TieredFileSystemDriver.OpenFile returned nil")
		}

		// Should be able to write to the file
		n, err := tieredFile.Write([]byte("test"))

		if err != nil {
			t.Error(err)
		}

		if n != 4 {
			t.Errorf("TieredFileSystemDriver.Write returned incorrect number of bytes, got %d", n)
		}

		// Create a new file
		_, err = tieredFileSystemDriver.Create("test3")

		if err != nil {
			t.Error(err)
		}

		// Test opening a read write file
		tieredFile, err = tieredFileSystemDriver.OpenFile("test3", os.O_RDWR, 0644)

		if err != nil {
			t.Error(err)
		}

		if tieredFile == nil {
			t.Error("TieredFileSystemDriver.OpenFile returned nil")
		}

		// Should be able to read and write to the file
		tieredFile.Write([]byte("test"))

		tieredFile.Seek(0, io.SeekStart)

		data := make([]byte, 4)

		_, err = tieredFile.Read(data)

		if err != nil {
			t.Error(err)
		}

		// Test opening a file with create flag
		tieredFile, err = tieredFileSystemDriver.OpenFile("test4", os.O_CREATE, 0644)

		if err != nil {
			t.Error(err)
		}

		if tieredFile == nil {
			t.Error("TieredFileSystemDriver.OpenFile returned nil")
		}

		// Test opening a file with create flag and read write flag
		tieredFile, err = tieredFileSystemDriver.OpenFile("test4", os.O_CREATE|os.O_RDWR, 0644)

		if err != nil {
			t.Error(err)
		}

		if tieredFile == nil {
			t.Error("TieredFileSystemDriver.OpenFile returned nil")
		}

		// Write to the file
		n, err = tieredFile.Write([]byte("test"))

		if err != nil {
			t.Error(err)
		}

		if n != 4 {
			t.Errorf("TieredFileSystemDriver.Write returned incorrect number of bytes, got %d", n)
		}

		// Test opening a file with append flag
		tieredFile, err = tieredFileSystemDriver.OpenFile("test5", os.O_CREATE|os.O_APPEND, 0644)

		if err != nil {
			t.Error(err)
		}

		if tieredFile == nil {
			t.Error("TieredFileSystemDriver.OpenFile returned nil")
		}

		// Write to the file
		n, err = tieredFile.Write([]byte("test"))

		if err == nil {
			t.Error("TieredFileSystemDriver.Write should return an error")
		}

		if n != 0 {
			t.Errorf("TieredFileSystemDriver.Write returned incorrect number of bytes, got %d", n)
		}

		// Test opening a file with truncate flag
		tieredFile, err = tieredFileSystemDriver.OpenFile("test6", os.O_CREATE|os.O_TRUNC, 0644)

		if err != nil {
			t.Error(err)
		}

		if tieredFile == nil {
			t.Error("TieredFileSystemDriver.OpenFile returned nil")
		}

		// Write to the file
		n, err = tieredFile.Write([]byte("test"))

		if err == nil {
			t.Error("TieredFileSystemDriver.Write should return an error")
		}

		if n != 0 {
			t.Errorf("TieredFileSystemDriver.Write returned incorrect number of bytes, got %d", n)
		}

		// Test opening a file with append and read write flags
		tieredFile, err = tieredFileSystemDriver.OpenFile("test7", os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)

		if err != nil {
			t.Error(err)
		}

		if tieredFile == nil {
			t.Error("TieredFileSystemDriver.OpenFile returned nil")
		}

		// Write to the file
		n, err = tieredFile.Write([]byte("test"))

		if err != nil {
			t.Error(err)
		}

		if n != 4 {
			t.Errorf("TieredFileSystemDriver.Write returned incorrect number of bytes, got %d", n)
		}

		// Read from the file
		tieredFile.Seek(0, io.SeekStart)

		data = make([]byte, 4)

		_, err = tieredFile.Read(data)

		if err != nil {
			t.Error(err)
		}

		if string(data) != "test" {
			t.Errorf("TieredFileSystemDriver.OpenFile returned incorrect data, got %s", data)
		}

		// Test opening a file with truncate and read write flags
		tieredFile, err = tieredFileSystemDriver.OpenFile("test8", os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0644)

		if err != nil {
			t.Error(err)
		}

		if tieredFile == nil {
			t.Error("TieredFileSystemDriver.OpenFile returned nil")
		}

		// Write to the file
		n, err = tieredFile.Write([]byte("test"))

		if err != nil {
			t.Error(err)
		}

		if n != 4 {
			t.Errorf("TieredFileSystemDriver.Write returned incorrect number of bytes, got %d", n)
		}

		// Read from the file
		tieredFile.Seek(0, io.SeekStart)

		data = make([]byte, 4)

		_, err = tieredFile.Read(data)

		if err != nil {
			t.Error(err)
		}

		if string(data) != "test" {
			t.Errorf("TieredFileSystemDriver.OpenFile returned incorrect data, got %s", data)
		}

		// Test opening a file with append and read only flags
		tieredFile, err = tieredFileSystemDriver.OpenFile("test9", os.O_CREATE|os.O_APPEND|os.O_RDONLY, 0644)

		if err != nil {
			t.Error(err)
		}

		if tieredFile == nil {
			t.Error("TieredFileSystemDriver.OpenFile returned nil")
		}

		// Write to the file
		n, err = tieredFile.Write([]byte("test"))

		if err == nil {
			t.Error("TieredFileSystemDriver.Write should return an error")
		}

		if n != 0 {
			t.Errorf("TieredFileSystemDriver.Write returned incorrect number of bytes, got %d", n)
		}

		// Test opening a file with truncate and read only flags
		tieredFile, err = tieredFileSystemDriver.OpenFile("test10", os.O_CREATE|os.O_TRUNC|os.O_RDONLY, 0644)

		if err != nil {
			t.Error(err)
		}

		if tieredFile == nil {
			t.Error("TieredFileSystemDriver.OpenFile returned nil")
		}

		// Write to the file
		n, err = tieredFile.Write([]byte("test"))

		if err == nil {
			t.Error("TieredFileSystemDriver.Write should return an error")
		}

		if n != 0 {
			t.Errorf("TieredFileSystemDriver.Write returned incorrect number of bytes, got %d", n)
		}

		// Test opening a file with append and write only flags
		tieredFile, err = tieredFileSystemDriver.OpenFile("test11", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)

		if err != nil {
			t.Error(err)
		}

		if tieredFile == nil {
			t.Error("TieredFileSystemDriver.OpenFile returned nil")
		}

		// Write to the file
		n, err = tieredFile.Write([]byte("test"))

		if err != nil {
			t.Error(err)
		}

		if n != 4 {
			t.Errorf("TieredFileSystemDriver.Write returned incorrect number of bytes, got %d", n)
		}

		// Read from the file
		tieredFile.Seek(0, io.SeekStart)

		data = make([]byte, 4)

		_, err = tieredFile.Read(data)

		if err != nil {
			t.Error(err)
		}

		if string(data) != "test" {
			t.Errorf("TieredFileSystemDriver.OpenFile returned incorrect data, got %s", data)
		}

		// Test opening a file with truncate and write only flags
		tieredFile, err = tieredFileSystemDriver.OpenFile("test12", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)

		if err != nil {
			t.Error(err)
		}

		if tieredFile == nil {
			t.Error("TieredFileSystemDriver.OpenFile returned nil")
		}

		// Write to the file
		n, err = tieredFile.Write([]byte("test"))

		if err != nil {
			t.Error(err)
		}

		if n != 4 {
			t.Errorf("TieredFileSystemDriver.Write returned incorrect number of bytes, got %d", n)
		}

		// Read from the file
		tieredFile.Seek(0, io.SeekStart)

		data = make([]byte, 4)

		_, err = tieredFile.Read(data)

		if err != nil {
			t.Error(err)
		}

		if string(data) != "test" {
			t.Errorf("TieredFileSystemDriver.OpenFile returned incorrect data, got %s", data)
		}
	})
}

func TestTieredFileSystemDriverKeepsCountOfOpenFiles(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		tieredFileSystemDriver := storage.NewTieredFileSystemDriver(
			context.Background(),
			storage.NewLocalFileSystemDriver(app.Config.DataPath+"/local"),
			storage.NewLocalFileSystemDriver(app.Config.DataPath+"/object"),
		)

		tieredFile, err := tieredFileSystemDriver.Create("test.txt")

		if err != nil {
			t.Error(err)
		}

		if tieredFile == nil {
			t.Error("TieredFileSystemDriver.Create returned nil")
		}

		if tieredFileSystemDriver.FileCount != 1 {
			t.Errorf("TieredFileSystemDriver.OpenFiles returned incorrect number of files, got %d", tieredFileSystemDriver.FileCount)
		}

		tieredFile.Close()

		if tieredFileSystemDriver.FileCount != 0 {
			t.Errorf("TieredFileSystemDriver.OpenFiles returned incorrect number of files, got %d", tieredFileSystemDriver.FileCount)
		}
	})
}

func TestTieredFileSystemDriverOnlyKeepsMaxFilesOpened(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		tieredFileSystemDriver := storage.NewTieredFileSystemDriver(
			context.Background(),
			storage.NewLocalFileSystemDriver(app.Config.DataPath+"/local"),
			storage.NewLocalFileSystemDriver(app.Config.DataPath+"/object"),
			func(context context.Context, tieredFileSystemDriver *storage.TieredFileSystemDriver) {
				tieredFileSystemDriver.MaxFilesOpened = 4
				tieredFileSystemDriver.WriteInterval = time.Millisecond * 500
			},
		)

		files := []string{
			"test1.txt",
			"test2.txt",
			"test3.txt",
			"test4.txt",
			"test5.txt",
			"test6.txt",
		}

		tieredFiles := make([]internalStorage.File, 6)
		var err error

		for i, file := range files {
			tieredFiles[i], err = tieredFileSystemDriver.Create(file)

			if err != nil {
				t.Error(err)
			}

			if tieredFiles[i] == nil {
				t.Error("TieredFileSystemDriver.Create returned nil")
			}
		}

		if tieredFileSystemDriver.FileCount != 4 {
			t.Fatalf("TieredFileSystemDriver.OpenFiles returned incorrect number of files, got %d", tieredFileSystemDriver.FileCount)
		}

		// Now there may be files that are out in the wild, when a closed file
		// is used an error will not be thrown, but the file will be reopened.
		for i := range tieredFiles {
			// Attempt to write to the file
			_, err = tieredFiles[i].Write([]byte("test"))

			// The first two files should return an error
			if err != nil {
				t.Error(err)
			}
		}

		time.Sleep(time.Second * 1)

		// Flushing the files should remove the closed files
		tieredFileSystemDriver.Flush()

		// The number of files should not have been changed
		if tieredFileSystemDriver.FileCount != 4 {
			t.Fatalf("TieredFileSystemDriver.OpenFiles returned incorrect number of files, got %d", tieredFileSystemDriver.FileCount)
		}
	})
}
