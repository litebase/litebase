package http_test

import (
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/sqlite3"
)

func TestDatabaseExportPartController(t *testing.T) {
	test.Run(t, func() {
		testServer := test.NewTestServer(t)
		defer testServer.Shutdown()

		testDatabase := test.MockDatabase(testServer.App)

		con, err := testServer.App.DatabaseManager.ConnectionManager().Get(testDatabase.DatabaseID, testDatabase.DatabaseBranchID)

		if err != nil {
			t.Fatal(err)
		}

		defer testServer.App.DatabaseManager.ConnectionManager().Release(con)

		// Create a table and insert some data
		_, err = con.GetConnection().Exec("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)", nil)

		if err != nil {
			t.Fatal(err)
		}

		err = con.GetConnection().Begin()

		if err != nil {
			t.Fatal(err)
		}

		for i := range 1000 {
			_, err = con.GetConnection().Exec("INSERT INTO test_table (name) VALUES (?)", []sqlite3.StatementParameter{
				{Value: fmt.Sprintf("test_name_%d", i)},
			})

			if err != nil {
				t.Fatal(err)
			}
		}

		err = con.GetConnection().Commit()

		if err != nil {
			t.Fatal(err)
		}

		client := testServer.WithAccessKeyClient([]auth.Statement{
			{
				Effect:   auth.StatementEffectAllow,
				Resource: "*",
				Actions:  []auth.Privilege{auth.DatabasePrivilegeExport},
			},
		})

		t.Run("DatabaseExportPartControllerShow_Success", func(t *testing.T) {
			// Start an export
			exportResp, statusCode, err := client.Send(
				fmt.Sprintf(
					"/v1/databases/%s/branches/%s/export",
					testDatabase.DatabaseName,
					testDatabase.BranchName,
				),
				"POST",
				nil,
			)

			if err != nil {
				t.Fatal(err)
			}

			if statusCode != http.StatusCreated {
				t.Fatalf("Expected export status 201, got %d", statusCode)
			}

			exportID := exportResp["data"].(map[string]any)["id"].(string)

			// Cleanup function
			defer func() {
				_, _, _ = client.Send(
					fmt.Sprintf(
						"/v1/databases/%s/branches/%s/export/%s/end",
						testDatabase.DatabaseName,
						testDatabase.BranchName,
						exportID,
					),
					"POST",
					nil,
				)
			}()

			// Request a range part - make a manual request for binary data
			partURL := fmt.Sprintf(
				"%s/v1/databases/%s/branches/%s/export/%s/ranges/1",
				testServer.Server.URL,
				testDatabase.DatabaseName,
				testDatabase.BranchName,
				exportID,
			)

			partReq, err := http.NewRequest("GET", partURL, nil)

			if err != nil {
				t.Fatal(err)
			}

			// Sign the request
			partHeaders := map[string]string{
				"Host":            partReq.URL.Host,
				"Content-Type":    "application/json",
				"X-Litebase-Date": fmt.Sprintf("%d", time.Now().UTC().Unix()),
			}

			for k, v := range partHeaders {
				partReq.Header.Set(k, v)
			}

			partSignature := auth.SignRequest(
				client.AccessKey.AccessKeyID,
				client.AccessKey.AccessKeySecret,
				partReq.Method,
				partReq.URL.Path,
				partHeaders,
				nil,
				map[string]string{},
			)

			partReq.Header.Set("Authorization", fmt.Sprintf("Litebase-HMAC-SHA256 %s", partSignature))

			httpClient := &http.Client{}
			partResp, err := httpClient.Do(partReq)

			if err != nil {
				t.Fatal(err)
			}

			defer func() {
				err := partResp.Body.Close()

				if err != nil {
					t.Fatal(err)
				}
			}()

			if partResp.StatusCode != http.StatusOK {
				t.Fatalf("Expected part status 200, got %d", partResp.StatusCode)
			}

			// Verify content type
			contentType := partResp.Header.Get("Content-Type")

			if contentType != "application/octet-stream" {
				t.Fatalf("Expected content type 'application/octet-stream', got '%s'", contentType)
			}

			// Read the range data
			rangeData, err := io.ReadAll(partResp.Body)

			if err != nil {
				t.Fatal(err)
			}

			if len(rangeData) == 0 {
				t.Fatal("Expected range data to be non-empty")
			}

			// Verify the size of the data. This value is based on manually
			// checking the size of the database from this test.
			if len(rangeData) != 16384 {
				t.Fatalf("Expected range data to be 16384 bytes, got %d", len(rangeData))
			}
		})

		t.Run("DatabaseExportPartControllerShow_InvalidExportID", func(t *testing.T) {
			// Request a range with a wrong export ID
			wrongExportID := "wrong-export-id"

			_, statusCode, _ := client.Send(
				fmt.Sprintf(
					"/v1/databases/%s/branches/%s/export/%s/ranges/1",
					testDatabase.DatabaseName,
					testDatabase.BranchName,
					wrongExportID,
				),
				"GET",
				nil,
			)

			if statusCode != http.StatusNotFound {
				t.Fatalf("Expected status 404 for wrong export ID, got %d", statusCode)
			}
		})

		t.Run("DatabaseExportPartControllerShow_InvalidRangeNumber", func(t *testing.T) {
			// Start an export
			exportResp, statusCode, err := client.Send(
				fmt.Sprintf(
					"/v1/databases/%s/branches/%s/export",
					testDatabase.DatabaseName,
					testDatabase.BranchName,
				),
				"POST",
				nil,
			)

			if err != nil {
				t.Fatal(err)
			}

			if statusCode != http.StatusCreated {
				t.Fatalf("Expected export status 201, got %d", statusCode)
			}

			exportID := exportResp["data"].(map[string]any)["id"].(string)

			// Request a range that doesn't exist (range 9999)
			_, statusCode, _ = client.Send(
				fmt.Sprintf(
					"/v1/databases/%s/branches/%s/export/%s/ranges/9999",
					testDatabase.DatabaseName,
					testDatabase.BranchName,
					exportID,
				),
				"GET",
				nil,
			)

			if statusCode != http.StatusNotFound {
				t.Fatalf("Expected status 404 for invalid range, got %d", statusCode)
			}
		})

		t.Run("DatabaseExportPartControllerShow_NoActiveExport", func(t *testing.T) {
			// Try to request a range without starting an export first
			_, statusCode, _ := client.Send(
				fmt.Sprintf(
					"/v1/databases/%s/branches/%s/export/some-id/ranges/1",
					testDatabase.DatabaseName,
					testDatabase.BranchName,
				),
				"GET",
				nil,
			)

			if statusCode != http.StatusNotFound {
				t.Fatalf("Expected status 404 when no export is active, got %d", statusCode)
			}
		})
	})
}
