package cmd_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
)

func TestImportCmd(t *testing.T) {
	t.Run("SuccessfulImport", func(t *testing.T) {
		test.Run(t, func() {
			server := test.NewTestServer(t)
			defer server.Shutdown()

			cli := test.NewTestCLI(t, server.App).
				WithServer(server).
				WithAccessKey([]auth.Statement{
					{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
				})

			// Create a temporary SQLite file
			tmpDir := t.TempDir()
			testFile := filepath.Join(tmpDir, "test.sqlite")
			testData := make([]byte, 1024*100) // 100KB test file

			for i := range testData {
				testData[i] = byte(i % 256)
			}

			if err := os.WriteFile(testFile, testData, 0644); err != nil {
				t.Fatalf("failed to create test file: %v", err)
			}

			err := cli.Run("import", testFile, "testdb/main")

			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}

			if cli.DoesNotSee("Import created with ID") {
				t.Error("expected output to contain 'Import created with ID'")
			}

			if cli.DoesNotSee("Uploaded chunk") {
				t.Error("expected output to contain 'Uploaded chunk'")
			}

			if cli.DoesNotSee("Successfully imported") {
				t.Error("expected output to contain 'Successfully imported'")
			}
		})
	})

	t.Run("FileNotFound", func(t *testing.T) {
		test.Run(t, func() {
			server := test.NewTestServer(t)
			defer server.Shutdown()

			cli := test.NewTestCLI(t, server.App).
				WithServer(server).
				WithAccessKey([]auth.Statement{
					{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
				})

			err := cli.Run("import", "/nonexistent/file.sqlite", "testdb")

			if err == nil {
				t.Fatal("expected error for nonexistent file")
			}
		})
	})

	t.Run("InvalidDatabasePath", func(t *testing.T) {
		test.Run(t, func() {
			server := test.NewTestServer(t)
			defer server.Shutdown()

			cli := test.NewTestCLI(t, server.App).
				WithServer(server).
				WithAccessKey([]auth.Statement{
					{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
				})

			tmpDir := t.TempDir()
			testFile := filepath.Join(tmpDir, "test.sqlite")

			if err := os.WriteFile(testFile, []byte("test"), 0644); err != nil {
				t.Fatalf("failed to create test file: %v", err)
			}

			err := cli.Run("import", testFile, "db/branch/extra")

			if err == nil {
				t.Fatal("expected error for invalid database path")
			}
		})
	})

	t.Run("LargeFileMultipleChunks", func(t *testing.T) {
		test.Run(t, func() {
			server := test.NewTestServer(t)
			defer server.Shutdown()

			cli := test.NewTestCLI(t, server.App).
				WithServer(server).
				WithAccessKey([]auth.Statement{
					{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
				})

			// Create a larger file that requires multiple chunks
			tmpDir := t.TempDir()
			testFile := filepath.Join(tmpDir, "large.sqlite")

			// Create 35MB file (should result in 3 chunks of 16MB each)
			file, err := os.Create(testFile)

			if err != nil {
				t.Fatalf("failed to create test file: %v", err)
			}

			// Write in chunks to avoid memory issues
			chunkData := make([]byte, 1024*1024) // 1MB at a time

			for range 35 {
				if _, err := file.Write(chunkData); err != nil {
					t.Fatalf("failed to write test data: %v", err)
				}
			}

			err = file.Close()

			if err != nil {
				t.Fatalf("failed to close test file: %v", err)
			}

			err = cli.Run("import", testFile, "largedb")

			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}

			if cli.DoesNotSee("Import created with ID") {
				t.Error("expected output to contain 'Import created with ID'")
			}

			if cli.DoesNotSee("in 3 chunks") {
				t.Error("expected output to indicate 3 chunks")
			}

			if cli.DoesNotSee("Successfully imported") {
				t.Error("expected output to contain 'Successfully imported'")
			}
		})
	})

	t.Run("ConcurrentUpload", func(t *testing.T) {
		test.Run(t, func() {
			server := test.NewTestServer(t)
			defer server.Shutdown()

			cli := test.NewTestCLI(t, server.App).
				WithServer(server).
				WithAccessKey([]auth.Statement{
					{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
				})

			// Create a file that requires multiple chunks
			tmpDir := t.TempDir()
			testFile := filepath.Join(tmpDir, "concurrent.sqlite")

			// Create 50MB file (should result in 4 chunks)
			file, err := os.Create(testFile)

			if err != nil {
				t.Fatalf("failed to create test file: %v", err)
			}

			chunkData := make([]byte, 1024*1024) // 1MB at a time

			for range 50 {
				if _, err := file.Write(chunkData); err != nil {
					t.Fatalf("failed to write test data: %v", err)
				}
			}

			err = file.Close()

			if err != nil {
				t.Fatalf("failed to close test file: %v", err)
			}

			err = cli.Run("import", testFile, "concurrentdb", "--concurrency", "3")

			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}

			if cli.DoesNotSee("Import created with ID") {
				t.Error("expected output to contain 'Import created with ID'")
			}

			if cli.DoesNotSee("concurrency=3") {
				t.Error("expected output to indicate concurrency=3")
			}

			if cli.DoesNotSee("Successfully imported") {
				t.Error("expected output to contain 'Successfully imported'")
			}
		})
	})
}
