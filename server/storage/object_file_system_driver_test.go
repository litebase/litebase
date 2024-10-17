package storage_test

import (
	"io"
	"litebase/internal/test"
	"litebase/server"
	"litebase/server/storage"
	"os"
	"testing"
)

func TestObjectFileSystemDriver(t *testing.T) {
	test.RunWithObjectStorage(t, func(app *server.App) {
		driver := storage.NewObjectFileSystemDriver(app.Config)

		if driver == nil {
			t.Fatal("Expected driver to be initialized")
		}
	})
}

func TestObectFileSystemDriverCreate(t *testing.T) {
	test.RunWithObjectStorage(t, func(app *server.App) {
		driver := storage.NewObjectFileSystemDriver(app.Config)

		// Test creating a file
		file, err := driver.Create("test.txt")

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if file == nil {
			t.Fatal("Expected file to be created")
		}
	})
}

func TestObjectFileSystemDriverEnsureBucketExists(t *testing.T) {
	test.RunWithObjectStorage(t, func(app *server.App) {
		driver := storage.NewObjectFileSystemDriver(app.Config)

		// Ensure the bucket exists
		driver.EnsureBucketExists()

		s3Client := storage.NewS3Client(
			app.Config,
			app.Config.StorageBucket,
			app.Config.StorageRegion,
		)

		// Check if the bucket exists
		_, err := s3Client.HeadBucket()

		if err != nil {
			t.Fatalf("Expected bucket to exist, got error: %v", err)
		}
	})
}

func TestObjectFileSystemDriverMkdir(t *testing.T) {
	test.RunWithObjectStorage(t, func(app *server.App) {
		driver := storage.NewObjectFileSystemDriver(app.Config)

		// Test creating a directory
		err := driver.Mkdir("testdir", 0755)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}
	})
}

func TestObjectFileSystemDriverMkdirAll(t *testing.T) {
	test.RunWithObjectStorage(t, func(app *server.App) {
		driver := storage.NewObjectFileSystemDriver(app.Config)

		// Test creating a directory and all parent directories
		err := driver.MkdirAll("testdir/subdir", 0755)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}
	})
}

func TestObjectFileSystemDriverOpen(t *testing.T) {
	test.RunWithObjectStorage(t, func(app *server.App) {
		driver := storage.NewObjectFileSystemDriver(app.Config)

		// Create a file to open
		_, err := driver.Create("test.txt")

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		// Test opening the file
		file, err := driver.Open("test.txt")

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if file == nil {
			t.Fatal("Expected file to be opened")
		}
	})
}

func TestObjectFileSystemDriverOpenFile(t *testing.T) {
	test.RunWithObjectStorage(t, func(app *server.App) {
		driver := storage.NewObjectFileSystemDriver(app.Config)

		// Create a file to open
		_, err := driver.Create("test.txt")

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		// Test opening the file
		file, err := driver.OpenFile("test.txt", os.O_RDONLY, 0644)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if file == nil {
			t.Fatal("Expected file to be opened")
		}
	})
}

func TestObjectFileSystemDriverReadDir(t *testing.T) {
	test.RunWithObjectStorage(t, func(app *server.App) {
		driver := storage.NewObjectFileSystemDriver(app.Config)

		// Test reading a directory
		entries, err := driver.ReadDir("/")

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if len(entries) == 0 {
			t.Fatal("Expected entries to be returned")
		}
	})
}

func TestObjectFileSystemDriverReadFile(t *testing.T) {
	test.RunWithObjectStorage(t, func(app *server.App) {
		driver := storage.NewObjectFileSystemDriver(app.Config)

		err := driver.WriteFile("test.txt", []byte("Hello, World!"), 0644)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		// Test reading the file
		data, err := driver.ReadFile("test.txt")

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if err != nil && err != io.EOF {
			t.Fatalf("Expected no error, got %v", err)
		}

		if string(data) != "Hello, World!" {
			t.Fatalf("Expected file content to be 'Hello, World!', got '%s'", string(data))
		}
	})
}

func TestObjectFileSystemDriverRemove(t *testing.T) {
	test.RunWithObjectStorage(t, func(app *server.App) {
		driver := storage.NewObjectFileSystemDriver(app.Config)

		// Create a file to remove
		_, err := driver.Create("test.txt")

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		// Test removing the file
		err = driver.Remove("test.txt")

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}
	})
}

func TestObjectFileSystemDriverRemoveAll(t *testing.T) {
	test.RunWithObjectStorage(t, func(app *server.App) {
		driver := storage.NewObjectFileSystemDriver(app.Config)

		// Create a directory to remove
		err := driver.Mkdir("testdir", 0755)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		// Create a file in the directory
		_, err = driver.Create("testdir/test.txt")

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		// Test removing the directory
		err = driver.RemoveAll("testdir/")

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		// Check if the directory is removed
		entries, err := driver.ReadDir("testdir/")

		if err != nil {
			t.Fatal("Expected error when reading removed directory")
		}

		if len(entries) != 0 {
			t.Fatalf("Expected no entries, got %d", len(entries))
		}
	})
}

func TestObjectFileSystemDriverRename(t *testing.T) {
	test.RunWithObjectStorage(t, func(app *server.App) {
		driver := storage.NewObjectFileSystemDriver(app.Config)

		// Create a file to rename
		_, err := driver.Create("test.txt")

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		// Test renaming the file
		err = driver.Rename("test.txt", "test_renamed.txt")

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		// Check if the old file name no longer exists
		s3Client := storage.NewS3Client(
			app.Config,
			app.Config.StorageBucket,
			app.Config.StorageRegion,
		)

		_, err = s3Client.GetObject("test.txt")

		if err == nil {
			t.Fatal("Expected error when accessing renamed file")
		}

		// Check if the new file name exists
		_, err = s3Client.GetObject("test_renamed.txt")

		if err != nil {
			t.Fatal("Expected renamed file to exist")
		}
	})
}

func TestObjectFileSystemDriverStat(t *testing.T) {
	test.RunWithObjectStorage(t, func(app *server.App) {
		driver := storage.NewObjectFileSystemDriver(app.Config)

		// Create a file to stat
		_, err := driver.Create("test.txt")

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		// Test statting the file
		fileInfo, err := driver.Stat("test.txt")

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if fileInfo == nil {
			t.Fatal("Expected file info to be returned")
		}
	})
}

func TestObjectFileSystemDriverTruncate(t *testing.T) {
	test.RunWithObjectStorage(t, func(app *server.App) {
		driver := storage.NewObjectFileSystemDriver(app.Config)

		// Create a file to truncate
		err := driver.WriteFile("test.txt", []byte("Hello, World!"), 0644)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		// Test truncating the file
		err = driver.Truncate("test.txt", 5)

		if err == nil {
			t.Fatal("Expected error when truncating file")
		}
	})
}

func TestObjectFileSystemDriverWriteFile(t *testing.T) {
	test.RunWithObjectStorage(t, func(app *server.App) {
		driver := storage.NewObjectFileSystemDriver(app.Config)

		// Test writing to a file
		err := driver.WriteFile("test.txt", []byte("Hello, World!"), 0644)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		// Check if the file content is correct
		data, err := driver.ReadFile("test.txt")

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if string(data) != "Hello, World!" {
			t.Fatalf("Expected file content to be 'Hello, World!', got '%s'", string(data))
		}
	})
}
