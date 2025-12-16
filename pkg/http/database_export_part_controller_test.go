package http_test

import (
	"context"
	"encoding/json"
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

			// First, start an export to get an export ID
			exportURL := fmt.Sprintf(
				"%s/v1/databases/%s/branches/%s/export",
				testServer.Server.URL,
				testDatabase.DatabaseName,
				testDatabase.BranchName,
			)

			exportReq, err := http.NewRequest("POST", exportURL, nil)

			if err != nil {
				t.Fatal(err)
			}

			// Sign the export request
			headers := map[string]string{
				"Host":            exportReq.URL.Host,
				"Content-Type":    "application/json",
				"X-Litebase-Date": fmt.Sprintf("%d", time.Now().UTC().Unix()),
			}

			for k, v := range headers {
				exportReq.Header.Set(k, v)
			}

			signature := auth.SignRequest(
				client.AccessKey.AccessKeyID,
				client.AccessKey.AccessKeySecret,
				exportReq.Method,
				exportReq.URL.Path,
				headers,
				nil,
				map[string]string{},
			)

			exportReq.Header.Set("Authorization", fmt.Sprintf("Litebase-HMAC-SHA256 %s", signature))

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			exportReq = exportReq.WithContext(ctx)

			// Start the export in a goroutine
			exportRespChan := make(chan *http.Response, 1)
			exportErrChan := make(chan error, 1)

			go func() {
				httpClient := &http.Client{}
				resp, err := httpClient.Do(exportReq)

				if err != nil {
					exportErrChan <- err
					return
				}

				exportRespChan <- resp
			}()

			// Wait for the export response
			var exportID string
			var exportResp *http.Response

			select {
			case exportResp = <-exportRespChan:
				if exportResp.StatusCode != http.StatusOK {
					body, err := io.ReadAll(exportResp.Body)

					if err != nil {
						if err := exportResp.Body.Close(); err != nil {
							t.Fatal(err)
						}

						t.Fatal(err)
					}

					err = exportResp.Body.Close()

					if err != nil {
						t.Fatal(err)
					}

					t.Fatalf("Expected export status 200, got %d: %s", exportResp.StatusCode, string(body))
				}

				// Read the export metadata
				var exportData map[string]any
				decoder := json.NewDecoder(exportResp.Body)

				if err := decoder.Decode(&exportData); err != nil {
					err := exportResp.Body.Close()

					if err != nil {
						t.Fatal(err)
					}

					t.Fatal(err)
				}

				exportID = exportData["id"].(string)

				// Keep the connection open for the compaction barrier
				// Give the server a moment to enter the compaction barrier
				time.Sleep(50 * time.Millisecond)

			case err := <-exportErrChan:
				t.Fatalf("Export request failed: %v", err)

			case <-time.After(5 * time.Second):
				t.Fatal("Timeout waiting for export")
			}

			// Ensure we close the export connection at the end
			defer func() {
				if exportResp != nil {
					if err := exportResp.Body.Close(); err != nil {
						t.Fatal(err)
					}
				}

				cancel() // Cancel the context to end the export
			}()

			// Now request a range part
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

			// Sign the part request
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
				if err := partResp.Body.Close(); err != nil {
					t.Fatal(err)
				}
			}()

			if partResp.StatusCode != http.StatusOK {
				body, err := io.ReadAll(partResp.Body)

				if err != nil {
					t.Fatal(err)
				}

				t.Fatalf("Expected part status 200, got %d: %s", partResp.StatusCode, string(body))
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
			// Start an export
			exportURL := fmt.Sprintf(
				"%s/v1/databases/%s/branches/%s/export",
				testServer.Server.URL,
				testDatabase.DatabaseName,
				testDatabase.BranchName,
			)

			exportReq, err := http.NewRequest("POST", exportURL, nil)

			if err != nil {
				t.Fatal(err)
			}

			// Sign the export request
			headers := map[string]string{
				"Host":            exportReq.URL.Host,
				"Content-Type":    "application/json",
				"X-Litebase-Date": fmt.Sprintf("%d", time.Now().UTC().Unix()),
			}

			for k, v := range headers {
				exportReq.Header.Set(k, v)
			}

			signature := auth.SignRequest(
				client.AccessKey.AccessKeyID,
				client.AccessKey.AccessKeySecret,
				exportReq.Method,
				exportReq.URL.Path,
				headers,
				nil,
				map[string]string{},
			)

			exportReq.Header.Set("Authorization", fmt.Sprintf("Litebase-HMAC-SHA256 %s", signature))

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			exportReq = exportReq.WithContext(ctx)

			exportRespChan := make(chan *http.Response, 1)
			exportErrChan := make(chan error, 1)

			go func() {
				httpClient := &http.Client{}
				resp, err := httpClient.Do(exportReq)

				if err != nil {
					exportErrChan <- err
					return
				}

				exportRespChan <- resp
			}()

			var exportResp *http.Response

			select {
			case exportResp = <-exportRespChan:
				if exportResp.StatusCode != http.StatusOK {
					body, err := io.ReadAll(exportResp.Body)

					if err != nil {
						t.Fatal(err)
					}

					if err := exportResp.Body.Close(); err != nil {
						t.Fatal(err)
					}

					t.Fatalf("Expected export status 200, got %d: %s", exportResp.StatusCode, string(body))
				}

				// Read the export metadata
				var exportData map[string]any

				decoder := json.NewDecoder(exportResp.Body)

				if err := decoder.Decode(&exportData); err != nil {
					if err := exportResp.Body.Close(); err != nil {
						t.Fatal(err)
					}

					t.Fatal(err)
				}

			case err := <-exportErrChan:
				t.Fatalf("Export request failed: %v", err)

			case <-time.After(5 * time.Second):
				t.Fatal("Timeout waiting for export")
			}

			defer func() {
				if exportResp != nil {
					if err := exportResp.Body.Close(); err != nil {
						t.Fatal(err)
					}
				}

				cancel()
			}()

			// Request a range with a wrong export ID
			wrongExportID := "wrong-export-id"

			partURL := fmt.Sprintf(
				"%s/v1/databases/%s/branches/%s/export/%s/ranges/1",
				testServer.Server.URL,
				testDatabase.DatabaseName,
				testDatabase.BranchName,
				wrongExportID,
			)

			partReq, err := http.NewRequest("GET", partURL, nil)

			if err != nil {
				t.Fatal(err)
			}

			// Sign the part request
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
				if err := partResp.Body.Close(); err != nil {
					t.Fatal(err)
				}
			}()

			if partResp.StatusCode != http.StatusNotFound {
				body, err := io.ReadAll(partResp.Body)

				if err != nil {
					t.Fatal(err)
				}

				t.Fatalf("Expected status 404 for wrong export ID, got %d: %s", partResp.StatusCode, string(body))
			}
		})

		t.Run("DatabaseExportPartControllerShow_InvalidRangeNumber", func(t *testing.T) {
			// Start an export
			exportURL := fmt.Sprintf(
				"%s/v1/databases/%s/branches/%s/export",
				testServer.Server.URL,
				testDatabase.DatabaseName,
				testDatabase.BranchName,
			)

			exportReq, err := http.NewRequest("POST", exportURL, nil)

			if err != nil {
				t.Fatal(err)
			}

			// Sign the export request
			headers := map[string]string{
				"Host":            exportReq.URL.Host,
				"Content-Type":    "application/json",
				"X-Litebase-Date": fmt.Sprintf("%d", time.Now().UTC().Unix()),
			}

			for k, v := range headers {
				exportReq.Header.Set(k, v)
			}

			signature := auth.SignRequest(
				client.AccessKey.AccessKeyID,
				client.AccessKey.AccessKeySecret,
				exportReq.Method,
				exportReq.URL.Path,
				headers,
				nil,
				map[string]string{},
			)

			exportReq.Header.Set("Authorization", fmt.Sprintf("Litebase-HMAC-SHA256 %s", signature))

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			exportReq = exportReq.WithContext(ctx)

			exportRespChan := make(chan *http.Response, 1)
			exportErrChan := make(chan error, 1)

			go func() {
				httpClient := &http.Client{}
				resp, err := httpClient.Do(exportReq)

				if err != nil {
					exportErrChan <- err
					return
				}

				exportRespChan <- resp
			}()

			var exportID string
			var exportResp *http.Response

			select {
			case exportResp = <-exportRespChan:
				if exportResp.StatusCode != http.StatusOK {
					body, err := io.ReadAll(exportResp.Body)
					if err != nil {
						t.Fatal(err)
					}

					if err := exportResp.Body.Close(); err != nil {
						t.Fatal(err)
					}

					t.Fatalf("Expected export status 200, got %d: %s", exportResp.StatusCode, string(body))
				}

				var exportData map[string]any
				decoder := json.NewDecoder(exportResp.Body)

				if err := decoder.Decode(&exportData); err != nil {
					if err := exportResp.Body.Close(); err != nil {
						t.Fatal(err)
					}

					t.Fatal(err)
				}

				exportID = exportData["id"].(string)

			case err := <-exportErrChan:
				t.Fatalf("Export request failed: %v", err)

			case <-time.After(5 * time.Second):
				t.Fatal("Timeout waiting for export")
			}

			defer func() {
				if exportResp != nil {
					if err := exportResp.Body.Close(); err != nil {
						t.Fatal(err)
					}
				}

				cancel()
			}()

			// Request a range that doesn't exist (range 9999)
			partURL := fmt.Sprintf(
				"%s/v1/databases/%s/branches/%s/export/%s/ranges/9999",
				testServer.Server.URL,
				testDatabase.DatabaseName,
				testDatabase.BranchName,
				exportID,
			)

			partReq, err := http.NewRequest("GET", partURL, nil)

			if err != nil {
				t.Fatal(err)
			}

			// Sign the part request
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
				if err := partResp.Body.Close(); err != nil {
					t.Fatal(err)
				}
			}()

			if partResp.StatusCode != http.StatusNotFound {
				body, err := io.ReadAll(partResp.Body)

				if err != nil {
					t.Fatal(err)
				}

				t.Fatalf("Expected status 404 for invalid range, got %d: %s", partResp.StatusCode, string(body))
			}
		})

		t.Run("DatabaseExportPartControllerShow_NoActiveExport", func(t *testing.T) {
			// Try to request a range without starting an export first
			partURL := fmt.Sprintf(
				"%s/v1/databases/%s/branches/%s/export/some-id/ranges/1",
				testServer.Server.URL,
				testDatabase.DatabaseName,
				testDatabase.BranchName,
			)

			partReq, err := http.NewRequest("GET", partURL, nil)

			if err != nil {
				t.Fatal(err)
			}

			// Sign the part request
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
				if err := partResp.Body.Close(); err != nil {
					t.Fatal(err)
				}
			}()

			if partResp.StatusCode != http.StatusNotFound {
				body, err := io.ReadAll(partResp.Body)

				if err != nil {
					t.Fatal(err)
				}

				t.Fatalf("Expected status 404 when no export is active, got %d: %s", partResp.StatusCode, string(body))
			}
		})
	})
}
