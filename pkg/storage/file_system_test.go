package storage_test

import (
	"fmt"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/config"
	"github.com/litebase/litebase/pkg/server"
	"github.com/litebase/litebase/pkg/storage"
)

func TestFileSystem(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		t.Run("NewFileSystem", func(t *testing.T) {
			driver := storage.NewLocalFileSystemDriver("test")
			fs := storage.NewFileSystem(driver)

			if fs == nil {
				t.Error("NewFileSystem() returned nil")
			}
		})

		t.Run("Create", func(t *testing.T) {
			{
				driver := storage.NewLocalFileSystemDriver(fmt.Sprintf("%s/%s", app.Config.DataPath, config.StorageModeLocal))

				fs := storage.NewFileSystem(driver)

				file, err := fs.Create("test")

				if err != nil {
					t.Errorf("Create() returned an error: %v", err)
				}

				if file == nil {
					t.Error("Create() returned nil")
				}
			}
		})

		t.Run("Mkdir", func(t *testing.T) {
			driver := storage.NewLocalFileSystemDriver(fmt.Sprintf("%s/%s", app.Config.DataPath, config.StorageModeLocal))

			fs := storage.NewFileSystem(driver)

			err := fs.Mkdir("test_mkdir", 0750)

			if err != nil {
				t.Errorf("Mkdir() returned an error: %v", err)
			}
		})

		t.Run("MkdirAll", func(t *testing.T) {
			driver := storage.NewLocalFileSystemDriver(fmt.Sprintf("%s/%s", app.Config.DataPath, config.StorageModeLocal))

			fs := storage.NewFileSystem(driver)

			err := fs.MkdirAll("test_mkdir_all", 0750)

			if err != nil {
				t.Errorf("MkdirAll() returned an error: %v", err)
			}
		})

		t.Run("Open", func(t *testing.T) {
			driver := storage.NewLocalFileSystemDriver(fmt.Sprintf("%s/%s", app.Config.DataPath, config.StorageModeLocal))

			fs := storage.NewFileSystem(driver)

			file, err := fs.Open("test_open")

			if err == nil {
				t.Errorf("Open() returned nil, expected an error")
			}

			if file != nil {
				t.Error("Open() returned a file, expected nil")
			}

			// Create the file
			_, err = fs.Create("test_open")

			if err != nil {
				t.Errorf("Create() returned an error: %v", err)
			}

			file, err = fs.Open("test_open")

			if err != nil {
				t.Errorf("Open() returned an error: %v", err)
			}

			if file == nil {
				t.Error("Open() returned nil")
			}
		})

		t.Run("OpenFile", func(t *testing.T) {
			driver := storage.NewLocalFileSystemDriver(fmt.Sprintf("%s/%s", app.Config.DataPath, config.StorageModeLocal))

			fs := storage.NewFileSystem(driver)

			file, err := fs.OpenFile("test_open_file", 0, 0750)

			if err == nil {
				t.Errorf("OpenFile() returned nil, expected an error")
			}

			if file != nil {
				t.Error("OpenFile() returned a file, expected nil")
			}

			// Create the file
			_, err = fs.Create("test_open_file")

			if err != nil {
				t.Errorf("Create() returned an error: %v", err)
			}

			file, err = fs.OpenFile("test_open_file", 0, 0750)

			if err != nil {
				t.Errorf("OpenFile() returned an error: %v", err)
			}

			if file == nil {
				t.Error("OpenFile() returned nil")
			}
		})

		t.Run("Path", func(t *testing.T) {
			driver := storage.NewLocalFileSystemDriver(fmt.Sprintf("%s/%s", app.Config.DataPath, config.StorageModeLocal))

			fs := storage.NewFileSystem(driver)

			path := fs.Path("test")

			if path == "" {
				t.Error("Path() returned an empty string")
			}
		})

		t.Run("ReadDir", func(t *testing.T) {
			driver := storage.NewLocalFileSystemDriver(fmt.Sprintf("%s/%s", app.Config.DataPath, config.StorageModeLocal))

			fs := storage.NewFileSystem(driver)

			// Add some files and folders to the directory
			directories := []string{"test1_readdir", "test2_readdir", "test3_readdir"}
			files := []string{"test1.txt", "test2.txt", "test3.txt"}

			for _, directory := range directories {
				err := fs.MkdirAll("readdir/"+directory, 0750)

				if err != nil {
					t.Errorf("Mkdir() returned an error: %v", err)
				}
			}

			for _, file := range files {
				_, err := fs.Create("readdir/" + file)

				if err != nil {
					t.Errorf("Create() returned an error: %v", err)
				}
			}

			entries, err := fs.ReadDir("readdir")

			if err != nil {
				t.Errorf("ReadDir() returned an error: %v", err)
			}

			if len(entries) != len(directories)+len(files) {
				t.Errorf("ReadDir() returned %d entries, expected %d", len(entries), len(directories)+len(files))
			}

			// Check if all the entries are present
			for _, directory := range directories {
				found := false

				for _, entry := range entries {
					if entry.Name() == directory {
						found = true

						break
					}
				}

				if !found {
					t.Errorf("ReadDir() did not return directory %s", directory)
				}
			}

			for _, file := range files {

				found := false

				for _, entry := range entries {
					if entry.Name() == file {
						found = true

						break
					}
				}

				if !found {
					t.Errorf("ReadDir() did not return file %s", file)
				}
			}
		})

		t.Run("ReadFile", func(t *testing.T) {
			driver := storage.NewLocalFileSystemDriver(fmt.Sprintf("%s/%s", app.Config.DataPath, config.StorageModeLocal))

			fs := storage.NewFileSystem(driver)

			// Create a file
			_, err := fs.Create("test")

			if err != nil {
				t.Errorf("Create() returned an error: %v", err)
			}

			data, err := fs.ReadFile("test")

			if err != nil {
				t.Errorf("ReadFile() returned an error: %v", err)
			}

			if len(data) != 0 {
				t.Errorf("ReadFile() returned %d bytes, expected 0", len(data))
			}
		})

		t.Run("Remove", func(t *testing.T) {
			driver := storage.NewLocalFileSystemDriver(fmt.Sprintf("%s/%s", app.Config.DataPath, config.StorageModeLocal))

			fs := storage.NewFileSystem(driver)

			// Create a file
			file, err := fs.Create("test")

			if err != nil {
				t.Errorf("Create() returned an error: %v", err)
			}

			err = file.Close()

			if err != nil {
				t.Errorf("Close() returned an error: %v", err)
			}

			err = fs.Remove("test")

			if err != nil {
				t.Errorf("Remove() returned an error: %v", err)
			}
		})

		t.Run("RemoveAll", func(t *testing.T) {
			driver := storage.NewLocalFileSystemDriver(fmt.Sprintf("%s/%s", app.Config.DataPath, config.StorageModeLocal))

			fs := storage.NewFileSystem(driver)

			// Create a file
			_, err := fs.Create("test.txt")

			if err != nil {
				t.Errorf("Create() returned an error: %v", err)
			}

			// Create a directory
			err = fs.Mkdir("test", 0750)

			if err != nil {
				t.Errorf("Mkdir() returned an error: %v", err)
			}

			err = driver.RemoveAll("")

			if err != nil {
				t.Errorf("RemoveAll() returned an error: %v", err)
			}

			// Check if the directory is removed
			_, err = fs.ReadDir("test")

			if err == nil {
				t.Error("ReadDir() did not return an error, expected an error")
			}

			// Check if the file is removed
			_, err = fs.ReadFile("test.txt")

			if err == nil {
				t.Error("ReadFile() did not return an error, expected an error")
			}
		})

		t.Run("Rename", func(t *testing.T) {
			driver := storage.NewLocalFileSystemDriver(fmt.Sprintf("%s/%s", app.Config.DataPath, config.StorageModeLocal))

			fs := storage.NewFileSystem(driver)

			// Create a file
			_, err := fs.Create("test")

			if err != nil {
				t.Errorf("Create() returned an error: %v", err)
			}

			err = fs.Rename("test", "test2")

			if err != nil {
				t.Errorf("Rename() returned an error: %v", err)
			}

			// Check if the file is renamed
			_, err = fs.ReadFile("test")

			if err == nil {
				t.Error("ReadFile() did not return an error, expected an error")
			}

			data, err := fs.ReadFile("test2")

			if err != nil {
				t.Errorf("ReadFile() returned an error: %v", err)
			}

			if len(data) != 0 {
				t.Errorf("ReadFile() returned %d bytes, expected 0", len(data))
			}
		})

		t.Run("Stat", func(t *testing.T) {
			driver := storage.NewLocalFileSystemDriver(fmt.Sprintf("%s/%s", app.Config.DataPath, config.StorageModeLocal))

			fs := storage.NewFileSystem(driver)

			// Create a file
			_, err := fs.Create("test")

			if err != nil {
				t.Errorf("Create() returned an error: %v", err)
			}

			info, err := fs.Stat("test")

			if err != nil {
				t.Errorf("Stat() returned an error: %v", err)
			}

			if info == nil {
				t.Error("Stat() returned nil")
			}
		})

		t.Run("Truncate", func(t *testing.T) {
			driver := storage.NewLocalFileSystemDriver(fmt.Sprintf("%s/%s", app.Config.DataPath, config.StorageModeLocal))

			fs := storage.NewFileSystem(driver)

			// Create a file
			_, err := fs.Create("test")

			if err != nil {
				t.Errorf("Create() returned an error: %v", err)
			}

			err = fs.Truncate("test", 1024)

			if err != nil {
				t.Errorf("Truncate() returned an error: %v", err)
			}
		})

		t.Run("WriteFile", func(t *testing.T) {
			driver := storage.NewLocalFileSystemDriver(fmt.Sprintf("%s/%s", app.Config.DataPath, config.StorageModeLocal))

			fs := storage.NewFileSystem(driver)

			err := fs.WriteFile("test", []byte("test"), 0750)

			if err != nil {
				t.Errorf("WriteFile() returned an error: %v", err)
			}
		})
	})
}
