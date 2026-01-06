package cmd_test

import (
	"context"
	"fmt"
	"io"
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

			// Create client for API calls
			client := server.WithAccessKeyClient([]auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

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

			// Step 1: Create export
			createExportResp, statusCode, err := client.Send("/v1/databases/exporttest/branches/main/export", "POST", nil)

			if err != nil {
				t.Fatalf("failed to create export: %v", err)
			}

			if statusCode != 201 {
				t.Fatalf("expected status 201, got %d. Response: %v", statusCode, createExportResp)
			}

			// Extract data from response
			data, ok := createExportResp["data"].(map[string]any)

			if !ok {
				t.Fatalf("export response missing data field. Response: %+v", createExportResp)
			}

			exportID, ok := data["id"].(string)

			if !ok {
				t.Fatalf("export data missing id field. Data: %+v", data)
			}

			rangeCountFloat, ok := data["rangeCount"].(float64)

			if !ok {
				t.Fatalf("export data missing rangeCount field. Data: %+v", data)
			}

			rangeCount := int(rangeCountFloat)

			if rangeCount < 1 {
				t.Fatalf("expected at least 1 range, got %d", rangeCount)
			}

			// Step 2: Download all ranges
			tmpDir = t.TempDir()
			exportFile := filepath.Join(tmpDir, "export.sqlite")
			outFile, err := os.Create(exportFile)

			if err != nil {
				t.Fatalf("failed to create output file: %v", err)
			}

			defer func() {
				if err := outFile.Close(); err != nil {
					t.Fatalf("failed to close output file: %v", err)
				}
			}()

			for i := 1; i <= rangeCount; i++ {
				rangePath := fmt.Sprintf("/v1/databases/exporttest/branches/main/export/%s/ranges/%d", exportID, i)
				rangeResp, err := client.DownloadBinary(rangePath)

				if err != nil {
					t.Fatalf("failed to download range %d: %v", i, err)
				}

				defer func() {
					if err := rangeResp.Body.Close(); err != nil {
						t.Fatalf("failed to close range response body: %v", err)
					}
				}()

				_, err = io.Copy(outFile, rangeResp.Body)

				if err != nil {
					t.Fatalf("failed to write range %d to file: %v", i, err)
				}
			}

			// Step 3: End export
			endExportPath := fmt.Sprintf("/v1/databases/exporttest/branches/main/export/%s/end", exportID)

			_, _, err = client.Send(endExportPath, "POST", nil)

			if err != nil {
				t.Fatalf("failed to end export: %v", err)
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
}
