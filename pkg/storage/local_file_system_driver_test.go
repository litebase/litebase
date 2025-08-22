package storage_test

import (
	"fmt"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/config"
	"github.com/litebase/litebase/pkg/server"
	"github.com/litebase/litebase/pkg/storage"
)

func TestLocalFileSystemDriver(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		t.Run("TestNewLocalFileSystemDriver", func(t *testing.T) {
			driver := storage.NewLocalFileSystemDriver(fmt.Sprintf("%s/%s", app.Config.DataPath, config.StorageModeLocal))

			if driver == nil {
				t.Error("NewLocalFileSystemDriver() returned nil")
			}
		})

		t.Run("Create", func(t *testing.T) {
			driver := storage.NewLocalFileSystemDriver(fmt.Sprintf("%s/%s", app.Config.DataPath, config.StorageModeLocal))

			file, err := driver.Create("test")

			if err != nil {
				t.Errorf("Create() returned an error: %v", err)
			}

			if file == nil {
				t.Error("Create() returned nil")
			}
		})

		t.Run("Mkdir", func(t *testing.T) {
			driver := storage.NewLocalFileSystemDriver(fmt.Sprintf("%s/%s", app.Config.DataPath, config.StorageModeLocal))

			err := driver.Mkdir("test_Mkdir", 0750)

			if err != nil {
				t.Errorf("Mkdir() returned an error: %v", err)
			}
		})

		t.Run("MkdirAll", func(t *testing.T) {
			driver := storage.NewLocalFileSystemDriver(fmt.Sprintf("%s/%s", app.Config.DataPath, config.StorageModeLocal))

			err := driver.MkdirAll("test_MkdirAll", 0750)

			if err != nil {
				t.Errorf("MkdirAll() returned an error: %v", err)
			}
		})

		t.Run("Open", func(t *testing.T) {
			driver := storage.NewLocalFileSystemDriver(fmt.Sprintf("%s/%s", app.Config.DataPath, config.StorageModeLocal))

			file, err := driver.Open("test_Open.txt")

			if err == nil {
				t.Errorf("Open() returned nil, expected an error")
			}

			if file != nil {
				t.Error("Open() returned nil")
			}

			// Create the file
			_, err = driver.Create("test.txt")

			if err != nil {
				t.Errorf("Create() returned an error: %v", err)
			}

			file, err = driver.Open("test.txt")

			if err != nil {
				t.Errorf("Open() returned an error: %v", err)
			}

			if file == nil {
				t.Error("Open() returned nil")
			}
		})

		t.Run("OpenFile", func(t *testing.T) {
			driver := storage.NewLocalFileSystemDriver(fmt.Sprintf("%s/%s", app.Config.DataPath, config.StorageModeLocal))

			file, err := driver.OpenFile("test_OpenFile.txt", 0, 0750)

			if err == nil {
				t.Errorf("OpenFile() returned nil, expected an error")
			}

			if file != nil {
				t.Error("OpenFile() returned nil")
			}

			// Create the file
			_, err = driver.Create("test.txt")

			if err != nil {
				t.Errorf("Create() returned an error: %v", err)
			}

			file, err = driver.OpenFile("test.txt", 0, 0750)

			if err != nil {
				t.Errorf("OpenFile() returned an error: %v", err)
			}

			if file == nil {
				t.Error("OpenFile() returned nil")
			}
		})

		t.Run("Path", func(t *testing.T) {
			driver := storage.NewLocalFileSystemDriver(fmt.Sprintf("%s/%s", app.Config.DataPath, config.StorageModeLocal))

			if _, err := driver.Create("test_Path.txt"); err != nil {
				t.Errorf("Create() returned an error: %v", err)
			}

			expectedPath := fmt.Sprintf("%s/%s/test_Path.txt", app.Config.DataPath, config.StorageModeLocal)

			// Test the driver path
			if driver.Path("test_Path.txt") != expectedPath {
				t.Errorf("Path() returned %s, expected %s", driver.Path("test_Path.txt"), expectedPath)
			}
		})

		t.Run("ReadDir", func(t *testing.T) {
			driver := storage.NewLocalFileSystemDriver(fmt.Sprintf("%s/%s", app.Config.DataPath, config.StorageModeLocal))
			if err := driver.Mkdir("test_ReadDir", 0750); err != nil {
				t.Errorf("Mkdir() returned an error: %v", err)
			}

			// Add some files and folders to the directory
			directories := []string{"test1", "test2", "test3"}
			files := []string{"test1.txt", "test2.txt", "test3.txt"}

			for _, directory := range directories {
				err := driver.Mkdir("test_ReadDir/"+directory, 0750)

				if err != nil {
					t.Errorf("Mkdir() returned an error: %v", err)
				}
			}

			for _, file := range files {
				_, err := driver.Create("test_ReadDir/" + file)

				if err != nil {
					t.Errorf("Create() returned an error: %v", err)
				}
			}

			entries, err := driver.ReadDir("test_ReadDir")

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

			// Create a file
			file, err := driver.Create("test.txt")

			if err != nil {
				t.Errorf("Create() returned an error: %v", err)
			}

			if file == nil {
				t.Error("Create() returned nil")
			}

			// Write some data to the file
			data := []byte("Hello, World!")
			_, err = file.Write(data)

			if err != nil {
				t.Errorf("Write() returned an error: %v", err)
			}

			// Close the file
			err = file.Close()

			if err != nil {
				t.Errorf("Close() returned an error: %v", err)
			}

			// Read the file
			file, err = driver.Open("test.txt")

			if err != nil {
				t.Errorf("Open() returned an error: %v", err)
			}

			if file == nil {
				t.Error("Open() returned nil")
			}

			readData := make([]byte, len(data))
			_, err = file.Read(readData)

			if err != nil {
				t.Errorf("Read() returned an error: %v", err)
			}

			if string(readData) != string(data) {
				t.Errorf("Read() returned %s, expected %s", string(readData), string(data))
			}
		})

		t.Run("Remove", func(t *testing.T) {
			driver := storage.NewLocalFileSystemDriver(fmt.Sprintf("%s/%s", app.Config.DataPath, config.StorageModeLocal))

			// Create a file
			file, err := driver.Create("test.txt")

			if err != nil {
				t.Errorf("Create() returned an error: %v", err)
			}

			if file == nil {
				t.Error("Create() returned nil")
			}

			// Close the file
			err = file.Close()

			if err != nil {
				t.Errorf("Close() returned an error: %v", err)
			}

			// Remove the file
			err = driver.Remove("test.txt")

			if err != nil {
				t.Errorf("Remove() returned an error: %v", err)
			}
		})

		t.Run("RemoveAll", func(t *testing.T) {
			driver := storage.NewLocalFileSystemDriver(fmt.Sprintf("%s/%s", app.Config.DataPath, config.StorageModeLocal))

			if err := driver.MkdirAll("removeall", 0750); err != nil {
				t.Errorf("MkdirAll() returned an error: %v", err)
			}

			// Create a file
			_, err := driver.Create("removeall/test.txt")

			if err != nil {
				t.Errorf("Create() returned an error: %v", err)
			}

			// Create a directory
			err = driver.Mkdir("removeall/test", 0750)

			if err != nil {
				t.Errorf("Mkdir() returned an error: %v", err)
			}

			err = driver.RemoveAll("removeall")

			if err != nil {
				t.Errorf("RemoveAll() returned an error: %v", err)
			}

			// Check if the directory is removed
			_, err = driver.ReadDir("removeall/test")

			if err == nil {
				t.Error("ReadDir() did not return an error, expected an error")
			}

			// Check if the file is removed
			_, err = driver.ReadFile("removeall/test.txt")

			if err == nil {
				t.Error("ReadFile() did not return an error, expected an error")
			}
		})

		t.Run("Rename", func(t *testing.T) {
			driver := storage.NewLocalFileSystemDriver(fmt.Sprintf("%s/%s", app.Config.DataPath, config.StorageModeLocal))

			// Create a file
			_, err := driver.Create("test.txt")

			if err != nil {
				t.Errorf("Create() returned an error: %v", err)
			}

			// Rename the file
			err = driver.Rename("test.txt", "test2.txt")

			if err != nil {
				t.Errorf("Rename() returned an error: %v", err)
			}

			// Check if the file is renamed
			_, err = driver.ReadFile("test.txt")

			if err == nil {
				t.Error("ReadFile() did not return an error, expected an error")
			}

			_, err = driver.ReadFile("test2.txt")

			if err != nil {
				t.Errorf("ReadFile() returned an error: %v", err)
			}
		})

		t.Run("Stat", func(t *testing.T) {
			driver := storage.NewLocalFileSystemDriver(fmt.Sprintf("%s/%s", app.Config.DataPath, config.StorageModeLocal))

			// Create a file
			_, err := driver.Create("test.txt")

			if err != nil {
				t.Errorf("Create() returned an error: %v", err)
			}

			// Get the file info
			info, err := driver.Stat("test.txt")

			if err != nil {
				t.Errorf("Stat() returned an error: %v", err)
			}

			if info == nil {
				t.Error("Stat() returned nil")
			}
		})

		t.Run("Truncate", func(t *testing.T) {
			driver := storage.NewLocalFileSystemDriver(fmt.Sprintf("%s/%s", app.Config.DataPath, config.StorageModeLocal))

			// Create a file
			file, err := driver.Create("test.txt")

			if err != nil {
				t.Errorf("Create() returned an error: %v", err)
			}

			if file == nil {
				t.Error("Create() returned nil")
			}

			// Write some data to the file
			data := []byte("Hello, World!")
			_, err = file.Write(data)

			if err != nil {
				t.Errorf("Write() returned an error: %v", err)
			}

			// Close the file
			err = file.Close()

			if err != nil {
				t.Errorf("Close() returned an error: %v", err)
			}

			// Truncate the file
			err = driver.Truncate("test.txt", 5)

			if err != nil {
				t.Errorf("Truncate() returned an error: %v", err)
			}

			// Read the file
			file, err = driver.Open("test.txt")

			if err != nil {
				t.Errorf("Open() returned an error: %v", err)
			}

			if file == nil {
				t.Error("Open() returned nil")
			}

			readData := make([]byte, 5)
			_, err = file.Read(readData)

			if err != nil {
				t.Errorf("Read() returned an error: %v", err)
			}

			if string(readData) != string(data[:5]) {
				t.Errorf("Read() returned %s, expected %s", string(readData), string(data[:5]))
			}
		})

		t.Run("WriteFile", func(t *testing.T) {
			driver := storage.NewLocalFileSystemDriver(fmt.Sprintf("%s/%s", app.Config.DataPath, config.StorageModeLocal))

			// Write some data to a file
			data := []byte("Hello, World!")
			err := driver.WriteFile("test.txt", data, 0750)

			if err != nil {
				t.Errorf("WriteFile() returned an error: %v", err)
			}

			// Read the file
			file, err := driver.Open("test.txt")

			if err != nil {
				t.Errorf("Open() returned an error: %v", err)
			}

			if file == nil {
				t.Error("Open() returned nil")
			}

			readData := make([]byte, len(data))
			_, err = file.Read(readData)

			if err != nil {
				t.Errorf("Read() returned an error: %v", err)
			}

			if string(readData) != string(data) {
				t.Errorf("Read() returned %s, expected %s", string(readData), string(data))
			}
		})
	})
}
