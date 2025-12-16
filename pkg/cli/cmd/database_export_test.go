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

func TestDatabaseExportCmd(t *testing.T) {
	t.Run("SuccessfulExport", func(t *testing.T) {
		test.Run(t, func() {
			server := test.NewTestServer(t)
			defer server.Shutdown()

			cli := test.NewTestCLI(t, server.App).
				WithServer(server).
				WithAccessKey([]auth.Statement{
					{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
				})

			// First, import a database to export
			tmpDir := t.TempDir()
			importFile := filepath.Join(tmpDir, "import.sqlite")
			createTestSQLiteFile(t, importFile, 1) // 1MB file

			err := cli.Run("import", importFile, "exporttest/main")

			if err != nil {
				t.Fatalf("failed to import test database: %v", err)
			}

			// Now export it
			exportFile := filepath.Join(tmpDir, "export.sqlite")
			err = cli.Run("database", "export", "exporttest/main", exportFile)

			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}

			// Verify output messages
			if cli.DoesNotSee("Export created with ID") {
				t.Error("expected output to contain 'Export created with ID'")
			}

			if cli.DoesNotSee("Downloaded range") {
				t.Error("expected output to contain 'Downloaded range'")
			}

			if cli.DoesNotSee("Merging ranges") {
				t.Error("expected output to contain 'Merging ranges'")
			}

			if cli.DoesNotSee("Successfully exported") {
				t.Error("expected output to contain 'Successfully exported'")
			}

			// Verify the exported file exists and is a valid SQLite database
			if _, err := os.Stat(exportFile); os.IsNotExist(err) {
				t.Fatal("exported file does not exist")
			}

			// Try to open the exported database
			ctx := context.Background()
			conn, err := sqlite3.Open(ctx, exportFile, "", sqlite3.SQLITE_OPEN_READONLY)

			if err != nil {
				t.Fatalf("failed to open exported database: %v", err)
			}

			defer func() {
				if err := conn.Close(); err != nil {
					t.Fatalf("failed to close exported database connection: %v", err)
				}
			}()

			// Verify it has the test_data table
			result := sqlite3.NewResult()
			stmt, _, err := conn.Prepare(ctx, "SELECT COUNT(*) FROM test_data")

			if err != nil {
				t.Fatalf("failed to query exported database: %v", err)
			}

			defer func() {
				if err := stmt.Finalize(); err != nil {
					t.Fatalf("failed to finalize statement: %v", err)
				}
			}()

			if err := stmt.Exec(result); err != nil {
				t.Fatalf("failed to execute query on exported database: %v", err)
			}

			if len(result.Rows) == 0 {
				t.Fatal("expected at least one row in count result")
			}
		})
	})

	t.Run("ExportWithAutoExtension", func(t *testing.T) {
		test.Run(t, func() {
			server := test.NewTestServer(t)
			defer server.Shutdown()

			cli := test.NewTestCLI(t, server.App).
				WithServer(server).
				WithAccessKey([]auth.Statement{
					{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
				})

			// Import a database
			tmpDir := t.TempDir()
			importFile := filepath.Join(tmpDir, "import.sqlite")
			createTestSQLiteFile(t, importFile, 1)

			err := cli.Run("import", importFile, "autoext/main")

			if err != nil {
				t.Fatalf("failed to import test database: %v", err)
			}

			// Export without .sqlite extension (should be added automatically)
			exportFile := filepath.Join(tmpDir, "export")
			err = cli.Run("database", "export", "autoext/main", exportFile)

			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}

			// Verify the file was created with .sqlite extension
			expectedPath := exportFile + ".sqlite"

			if _, err := os.Stat(expectedPath); os.IsNotExist(err) {
				t.Fatal("exported file does not exist with .sqlite extension")
			}
		})
	})

	t.Run("ExportNonexistentDatabase", func(t *testing.T) {
		test.Run(t, func() {
			server := test.NewTestServer(t)
			defer server.Shutdown()

			cli := test.NewTestCLI(t, server.App).
				WithServer(server).
				WithAccessKey([]auth.Statement{
					{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
				})

			tmpDir := t.TempDir()
			exportFile := filepath.Join(tmpDir, "export.sqlite")
			err := cli.Run("database", "export", "nonexistent/main", exportFile)

			if err == nil {
				t.Fatal("expected error for nonexistent database")
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
			exportFile := filepath.Join(tmpDir, "export.sqlite")
			err := cli.Run("database", "export", "invalid", exportFile)

			if err == nil {
				t.Fatal("expected error for invalid database path")
			}
		})
	})

	t.Run("ConcurrentDownload", func(t *testing.T) {
		test.Run(t, func() {
			server := test.NewTestServer(t)
			defer server.Shutdown()

			cli := test.NewTestCLI(t, server.App).
				WithServer(server).
				WithAccessKey([]auth.Statement{
					{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
				})

			// Import a larger database with multiple ranges
			tmpDir := t.TempDir()
			importFile := filepath.Join(tmpDir, "large.sqlite")
			createTestSQLiteFile(t, importFile, 20) // 20MB = multiple ranges

			err := cli.Run("import", importFile, "concurrent/main")

			if err != nil {
				t.Fatalf("failed to import test database: %v", err)
			}

			// Export with custom concurrency
			exportFile := filepath.Join(tmpDir, "export.sqlite")
			err = cli.Run("database", "export", "concurrent/main", exportFile, "--concurrency", "5")

			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}

			if cli.DoesNotSee("concurrency=5") {
				t.Error("expected output to indicate concurrency=5")
			}

			if cli.DoesNotSee("Successfully exported") {
				t.Error("expected output to contain 'Successfully exported'")
			}

			// Verify the exported file exists
			if _, err := os.Stat(exportFile); os.IsNotExist(err) {
				t.Fatal("exported file does not exist")
			}
		})
	})

	t.Run("ExportMultipleRanges", func(t *testing.T) {
		test.Run(t, func() {
			server := test.NewTestServer(t)
			defer server.Shutdown()

			cli := test.NewTestCLI(t, server.App).
				WithServer(server).
				WithAccessKey([]auth.Statement{
					{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
				})

			// Import a database that will have multiple ranges
			tmpDir := t.TempDir()
			importFile := filepath.Join(tmpDir, "multirange.sqlite")
			createTestSQLiteFile(t, importFile, 25) // 25MB = multiple ranges

			err := cli.Run("import", importFile, "multirange/main")

			if err != nil {
				t.Fatalf("failed to import test database: %v", err)
			}

			// Export it
			exportFile := filepath.Join(tmpDir, "export.sqlite")
			err = cli.Run("database", "export", "multirange/main", exportFile)

			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}

			// Verify range word is plural
			if cli.DoesNotSee("ranges") {
				t.Error("expected output to contain 'ranges' (plural)")
			}

			// Verify no temporary files remain
			matches, err := filepath.Glob(filepath.Join(tmpDir, "export_*"))

			if err != nil {
				t.Fatalf("failed to check for temporary files: %v", err)
			}

			if len(matches) > 0 {
				t.Errorf("expected no temporary range files, found %d", len(matches))
			}
		})
	})
}
