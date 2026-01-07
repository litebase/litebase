package cmd_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/sqlite3"
)

// createTestSQLiteFile creates a valid SQLite database file for testing
func createTestSQLiteFile(t *testing.T, path string, sizeInMB int) {
	t.Helper()

	// Create a new SQLite database
	ctx := context.Background()
	conn, err := sqlite3.Open(ctx, path, "", sqlite3.SQLITE_OPEN_CREATE|sqlite3.SQLITE_OPEN_READWRITE)

	if err != nil {
		t.Fatalf("failed to create SQLite database: %v", err)
	}

	// Set page size to 4096 and journal mode to DELETE
	if _, err := conn.Exec(ctx, "PRAGMA page_size = 4096"); err != nil {
		err := conn.Close()

		if err != nil {
			t.Logf("failed to close database after page size error: %v", err)
		}

		t.Fatalf("failed to set page size: %v", err)
	}

	if _, err := conn.Exec(ctx, "PRAGMA journal_mode = DELETE"); err != nil {
		err := conn.Close()

		if err != nil {
			t.Logf("failed to close database after journal mode error: %v", err)
		}

		t.Fatalf("failed to set journal mode: %v", err)
	}

	// Create a table to make the database non-empty
	if _, err := conn.Exec(ctx, "CREATE TABLE test_data (id INTEGER PRIMARY KEY, data BLOB)"); err != nil {
		err := conn.Close()

		if err != nil {
			t.Logf("failed to close database after create table error: %v", err)
		}

		t.Fatalf("failed to create table: %v", err)
	}

	// Insert data to reach the desired size (approximately)
	// Each row is about 1KB of data
	rowsNeeded := sizeInMB * 1024
	data := make([]byte, 1024)

	// Prepare statement once outside the loop
	stmt, _, err := conn.Prepare(ctx, "INSERT INTO test_data (data) VALUES (?)")

	if err != nil {
		if err := conn.Close(); err != nil {
			t.Logf("failed to close database after prepare statement error: %v", err)
		}

		t.Fatalf("failed to prepare statement: %v", err)
	}

	defer func() {
		if err := stmt.Finalize(); err != nil {
			t.Logf("failed to finalize statement: %v", err)
		}
	}()

	// Start a transaction for batch inserts
	if _, err := conn.Exec(ctx, "BEGIN TRANSACTION"); err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}

	for i := range rowsNeeded {
		// Fill with some pattern
		for j := range data {
			data[j] = byte((i + j) % 256)
		}

		result := sqlite3.NewResult()

		if err := stmt.Exec(result, sqlite3.StatementParameter{Type: sqlite3.ParameterTypeBlob, Value: data}); err != nil {
			if _, rollbackErr := conn.Exec(ctx, "ROLLBACK"); rollbackErr != nil {
				t.Logf("failed to rollback transaction: %v", rollbackErr)
			}

			if err := conn.Close(); err != nil {
				t.Logf("failed to close database after exec error: %v", err)
			}

			t.Fatalf("failed to insert data: %v", err)
		}
	}

	// Commit the transaction
	if _, err := conn.Exec(ctx, "COMMIT"); err != nil {
		t.Fatalf("failed to commit transaction: %v", err)
	}

	// Ensure everything is written
	if err := conn.CacheFlush(); err != nil {
		err := conn.Close()

		if err != nil {
			t.Logf("failed to close database after cache flush error: %v", err)
		}

		t.Fatalf("failed to flush cache: %v", err)
	}

	// Close the connection before returning
	if err := conn.Close(); err != nil {
		t.Fatalf("failed to close database: %v", err)
	}
}

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
			createTestSQLiteFile(t, testFile, 1) // 1MB file = 1 chunk

			err := cli.Run("import", testFile, "testdb/main")

			if err != nil {
				t.Fatalf("expected no error, got %v", err)
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
			createTestSQLiteFile(t, testFile, 20) // 20MB file = 2 chunks

			err := cli.Run("import", testFile, "largedb")

			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}

			if cli.DoesNotSee("in 2 chunks") {
				t.Error("expected output to indicate 2 chunks")
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
			createTestSQLiteFile(t, testFile, 25) // 25MB file = 2 chunks

			err := cli.Run("import", testFile, "concurrentdb", "--concurrency", "3")

			if err != nil {
				t.Fatalf("expected no error, got %v", err)
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
