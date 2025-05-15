package storage_test

import (
	"context"
	"io"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/server"
	"github.com/litebase/litebase/server/storage"
)

func TestObjectFileSystemDriver(t *testing.T) {
	test.RunWithObjectStorage(t, func(app *server.App) {
		driver := storage.NewObjectFileSystemDriver(app.Config)

		if driver == nil {
			t.Fatal("Expected driver to be initialized")
		}
	})
}

func TestObjectFileSystemDriver_ClearFiles(t *testing.T) {
	test.RunWithObjectStorage(t, func(app *server.App) {
		driver := storage.NewObjectFileSystemDriver(app.Config)

		// Test clearing files
		err := driver.ClearFiles()

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		// Check if the bucket is empty
		resp, err := driver.S3Client.ListObjectsV2(
			context.TODO(),
			&s3.ListObjectsV2Input{
				Bucket:    &app.Config.StorageBucket,
				Delimiter: aws.String("/"),
				Prefix:    aws.String(""),
			},
		)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if len(resp.Contents) != 0 {
			t.Fatal("Expected bucket to be empty")
		}
	})
}

func TestObectFileSystemDriver_Create(t *testing.T) {
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

func TestObjectFileSystemDriver_EnsureBucketExists(t *testing.T) {
	test.RunWithObjectStorage(t, func(app *server.App) {
		driver := storage.NewObjectFileSystemDriver(app.Config)

		// Ensure the bucket exists
		driver.EnsureBucketExists()

		s3Client := driver.S3Client

		// Check if the bucket exists
		_, err := s3Client.HeadBucket(
			context.TODO(),
			&s3.HeadBucketInput{
				Bucket: &app.Config.StorageBucket,
			},
		)

		if err != nil {
			t.Fatalf("Expected bucket to exist, got error: %v", err)
		}
	})
}

func TestObjectFileSystemDriver_Mkdir(t *testing.T) {
	test.RunWithObjectStorage(t, func(app *server.App) {
		driver := storage.NewObjectFileSystemDriver(app.Config)

		// Test creating a directory
		err := driver.Mkdir("testdir", 0755)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}
	})
}

func TestObjectFileSystemDriver_MkdirAll(t *testing.T) {
	test.RunWithObjectStorage(t, func(app *server.App) {
		driver := storage.NewObjectFileSystemDriver(app.Config)

		// Test creating a directory and all parent directories
		err := driver.MkdirAll("testdir/subdir", 0755)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}
	})
}

func TestObjectFileSystemDriver_Open(t *testing.T) {
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

func TestObjectFileSystemDriver_OpenFile(t *testing.T) {
	test.RunWithObjectStorage(t, func(app *server.App) {
		driver := storage.NewObjectFileSystemDriver(app.Config)

		// Create a file to open
		file, err := driver.Create("test.txt")

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		_, err = file.WriteString("Hello, World!")

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		// Close the file
		err = file.Close()

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		// Test opening the file
		file, err = driver.OpenFile("test.txt", os.O_CREATE|os.O_RDONLY, 0644)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if file == nil {
			t.Fatal("Expected file to be opened")
		}

		// Read the file content
		data := make([]byte, 13)

		n, err := file.Read(data)

		if err != nil && err != io.EOF {
			t.Fatalf("Expected no error, got %v", err)
		}

		if n != 13 {
			t.Fatalf("Expected to read 13 bytes, got %d", n)
		}

		if string(data) != "Hello, World!" {
			t.Fatalf("Expected file content to be 'Hello, World!', got '%s'", string(data))
		}
	})
}

func TestObjectFileSystemDriverReadDir(t *testing.T) {
	test.RunWithObjectStorage(t, func(app *server.App) {
		driver := storage.NewObjectFileSystemDriver(app.Config)

		// Test reading a directory
		entries, err := driver.ReadDir("/test")

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if len(entries) != 0 {
			t.Fatal("Expected no entries to be returned")
		}

		driver.Create("/test/test.txt")

		entries, err = driver.ReadDir("/test")

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if len(entries) == 0 {
			t.Fatal("Expected some entries to be returned")
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
		_, err = driver.S3Client.GetObject(context.TODO(),
			&s3.GetObjectInput{
				Bucket: &app.Config.StorageBucket,
				Key:    aws.String("test.txt"),
			},
		)

		if err == nil {
			t.Fatal("Expected error when accessing renamed file")
		}

		// Check if the new file name exists
		_, err = driver.S3Client.GetObject(context.TODO(),
			&s3.GetObjectInput{
				Bucket: &app.Config.StorageBucket,
				Key:    aws.String("test_renamed.txt"),
			},
		)

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
