package storage_test

import (
	"fmt"
	"litebase/internal/config"
	"litebase/internal/test"
	"litebase/server/storage"
	"testing"
)

func TestNewFileSystem(t *testing.T) {
	test.Run(t, func() {
		driver := storage.NewLocalFileSystemDriver("test")

		fs := storage.NewFileSystem(driver)

		if fs == nil {
			t.Error("NewFileSystem() returned nil")
		}
	})
}

func TestFileSystemCreate(t *testing.T) {
	test.Run(t, func() {
		driver := storage.NewLocalFileSystemDriver(fmt.Sprintf("%s/%s", config.Get().DataPath, config.STORAGE_MODE_LOCAL))

		fs := storage.NewFileSystem(driver)

		file, err := fs.Create("test")

		if err != nil {
			t.Errorf("Create() returned an error: %v", err)
		}

		if file == nil {
			t.Error("Create() returned nil")
		}
	})
}

func TestFileSystemMkdir(t *testing.T) {
	test.Run(t, func() {
		driver := storage.NewLocalFileSystemDriver(fmt.Sprintf("%s/%s", config.Get().DataPath, config.STORAGE_MODE_LOCAL))

		fs := storage.NewFileSystem(driver)

		err := fs.Mkdir("test", 0755)

		if err != nil {
			t.Errorf("Mkdir() returned an error: %v", err)
		}
	})
}

func TestFileSystemMkdirAll(t *testing.T) {
	test.Run(t, func() {
		driver := storage.NewLocalFileSystemDriver(fmt.Sprintf("%s/%s", config.Get().DataPath, config.STORAGE_MODE_LOCAL))

		fs := storage.NewFileSystem(driver)

		err := fs.MkdirAll("test", 0755)

		if err != nil {
			t.Errorf("MkdirAll() returned an error: %v", err)
		}
	})
}

func TestFileSystemOpen(t *testing.T) {
	test.Run(t, func() {
		driver := storage.NewLocalFileSystemDriver(fmt.Sprintf("%s/%s", config.Get().DataPath, config.STORAGE_MODE_LOCAL))

		fs := storage.NewFileSystem(driver)

		file, err := fs.Open("test")

		if err == nil {
			t.Errorf("Open() returned nil, expected an error")
		}

		if file == nil {
			t.Error("Open() returned nil")
		}

		// Create the file
		_, err = fs.Create("test")

		if err != nil {
			t.Errorf("Create() returned an error: %v", err)
		}

		file, err = fs.Open("test")

		if err != nil {
			t.Errorf("Open() returned an error: %v", err)
		}

		if file == nil {
			t.Error("Open() returned nil")
		}
	})
}

func TestFileSystemOpenFile(t *testing.T) {
	test.Run(t, func() {
		driver := storage.NewLocalFileSystemDriver(fmt.Sprintf("%s/%s", config.Get().DataPath, config.STORAGE_MODE_LOCAL))

		fs := storage.NewFileSystem(driver)

		file, err := fs.OpenFile("test", 0, 0755)

		if err == nil {
			t.Errorf("OpenFile() returned nil, expected an error")
		}

		if file == nil {
			t.Error("OpenFile() returned nil")
		}

		// Create the file
		_, err = fs.Create("test")

		if err != nil {
			t.Errorf("Create() returned an error: %v", err)
		}

		file, err = fs.OpenFile("test", 0, 0755)

		if err != nil {
			t.Errorf("OpenFile() returned an error: %v", err)
		}

		if file == nil {
			t.Error("OpenFile() returned nil")
		}
	})
}

func TestFileSystemReadDir(t *testing.T) {
	test.Run(t, func() {
		driver := storage.NewLocalFileSystemDriver(fmt.Sprintf("%s/%s", config.Get().DataPath, config.STORAGE_MODE_LOCAL))

		fs := storage.NewFileSystem(driver)

		// Add some files and folders to the directory
		directories := []string{"test1", "test2", "test3"}
		files := []string{"test1.txt", "test2.txt", "test3.txt"}

		for _, directory := range directories {
			err := fs.Mkdir(directory, 0755)

			if err != nil {
				t.Errorf("Mkdir() returned an error: %v", err)
			}
		}

		for _, file := range files {
			_, err := fs.Create(file)

			if err != nil {
				t.Errorf("Create() returned an error: %v", err)
			}
		}

		entries, err := fs.ReadDir("")

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
				if entry.Name == directory {
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
				if entry.Name == file {
					found = true

					break
				}
			}

			if !found {
				t.Errorf("ReadDir() did not return file %s", file)
			}
		}
	})
}

func TestFileSystemReadFile(t *testing.T) {
	test.Run(t, func() {
		driver := storage.NewLocalFileSystemDriver(fmt.Sprintf("%s/%s", config.Get().DataPath, config.STORAGE_MODE_LOCAL))

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
}

func TestFileSystemRemove(t *testing.T) {
	test.Run(t, func() {
		driver := storage.NewLocalFileSystemDriver(fmt.Sprintf("%s/%s", config.Get().DataPath, config.STORAGE_MODE_LOCAL))

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
}

func TestFileSystemDriverRemoveAll(t *testing.T) {
	test.Run(t, func() {
		driver := storage.NewLocalFileSystemDriver(fmt.Sprintf("%s/%s", config.Get().DataPath, config.STORAGE_MODE_LOCAL))

		fs := storage.NewFileSystem(driver)

		// Create a file
		_, err := fs.Create("test.txt")

		if err != nil {
			t.Errorf("Create() returned an error: %v", err)
		}

		// Create a directory
		err = fs.Mkdir("test", 0755)

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
}

func TestFileSystemRename(t *testing.T) {
	test.Run(t, func() {
		driver := storage.NewLocalFileSystemDriver(fmt.Sprintf("%s/%s", config.Get().DataPath, config.STORAGE_MODE_LOCAL))

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
}

func TestFileSystemStat(t *testing.T) {
	test.Run(t, func() {
		driver := storage.NewLocalFileSystemDriver(fmt.Sprintf("%s/%s", config.Get().DataPath, config.STORAGE_MODE_LOCAL))

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
}

func TestFileSystemTruncate(t *testing.T) {
	test.Run(t, func() {
		driver := storage.NewLocalFileSystemDriver(fmt.Sprintf("%s/%s", config.Get().DataPath, config.STORAGE_MODE_LOCAL))

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
}

func TestFileSystemWriteFile(t *testing.T) {
	test.Run(t, func() {
		driver := storage.NewLocalFileSystemDriver(fmt.Sprintf("%s/%s", config.Get().DataPath, config.STORAGE_MODE_LOCAL))

		fs := storage.NewFileSystem(driver)

		err := fs.WriteFile("test", []byte("test"), 0755)

		if err != nil {
			t.Errorf("WriteFile() returned an error: %v", err)
		}
	})
}
