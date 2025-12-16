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
)

func TestDatabaseExportController(t *testing.T) {
	test.Run(t, func() {
		testServer := test.NewTestServer(t)
		defer testServer.Shutdown()

		testDatabase := test.MockDatabase(testServer.App)

		client := testServer.WithAccessKeyClient([]auth.Statement{
			{
				Effect:   auth.StatementEffectAllow,
				Resource: "*",
				Actions:  []auth.Privilege{auth.DatabasePrivilegeExport},
			},
		})

		t.Run("DatabaseExportControllerStore", func(t *testing.T) {
			url := fmt.Sprintf(
				"/v1/databases/%s/branches/%s/export",
				testDatabase.DatabaseName,
				testDatabase.BranchName,
			)

			// Create an HTTP request to the actual server URL for streaming
			req, err := http.NewRequest("POST", fmt.Sprintf(
				"%s%s",
				testServer.Server.URL,
				url,
			), nil)

			if err != nil {
				t.Fatal(err)
			}

			// Use the client's access key to sign the request
			headers := map[string]string{
				"Host":            req.URL.Host,
				"Content-Type":    "application/json",
				"X-Litebase-Date": fmt.Sprintf("%d", time.Now().UTC().Unix()),
			}

			for k, v := range headers {
				req.Header.Set(k, v)
			}

			signature := auth.SignRequest(
				client.AccessKey.AccessKeyID,
				client.AccessKey.AccessKeySecret,
				req.Method,
				req.URL.Path,
				headers,
				nil,
				map[string]string{},
			)

			req.Header.Set("Authorization", fmt.Sprintf("Litebase-HMAC-SHA256 %s", signature))

			// Make the request in a goroutine so we can cancel it
			responseChan := make(chan *http.Response, 1)
			errorChan := make(chan error, 1)

			ctx, cancel := context.WithCancel(context.Background())
			req = req.WithContext(ctx)

			go func() {
				httpClient := &http.Client{
					Timeout: 5 * time.Second,
				}

				resp, err := httpClient.Do(req)

				if err != nil {
					errorChan <- err
					return
				}

				responseChan <- resp
			}()

			// Wait for the response
			select {
			case resp := <-responseChan:
				defer func() {
					err := resp.Body.Close()

					if err != nil {
						t.Fatal(err)
					}
				}()

				if resp.StatusCode != http.StatusOK {
					body, err := io.ReadAll(resp.Body)

					if err != nil {
						t.Fatal(err)
					}

					t.Fatalf("Expected status 200, got %d: %s", resp.StatusCode, string(body))
				}

				// Read the export metadata
				var exportData map[string]any

				decoder := json.NewDecoder(resp.Body)

				if err := decoder.Decode(&exportData); err != nil {
					t.Fatal(err)
				}

				// Verify the response contains the expected fields
				if exportData["id"] == nil {
					t.Fatal("Expected export response to have 'id' field")
				}

				if exportData["rangeCount"] == nil {
					t.Fatal("Expected export response to have 'rangeCount' field")
				}

				if exportData["startedAt"] == nil {
					t.Fatal("Expected export response to have 'startedAt' field")
				}

				// Verify rangeCount is a number
				rangeCount, ok := exportData["rangeCount"].(float64)

				if !ok {
					t.Fatal("Expected rangeCount to be a number")
				}

				if rangeCount == 0 {
					t.Fatal("Expected rangeCount to be greater than 0")
				}

				// Cancel the request to close the connection
				cancel()

			case err := <-errorChan:
				t.Fatal(err)

			case <-time.After(5 * time.Second):
				cancel()
				t.Fatal("Timeout waiting for export response")
			}
		})

		t.Run("DatabaseExportControllerStore_OnlyOneExportAllowed", func(t *testing.T) {
			url := fmt.Sprintf(
				"%s/v1/databases/%s/branches/%s/export",
				testServer.Server.URL,
				testDatabase.DatabaseName,
				testDatabase.BranchName,
			)

			// Start first export
			req1, err := http.NewRequest("POST", url, nil)

			if err != nil {
				t.Fatal(err)
			}

			req1Headers := map[string]string{
				"Host":            req1.URL.Host,
				"Content-Type":    "application/json",
				"X-Litebase-Date": fmt.Sprintf("%d", time.Now().UTC().Unix()),
			}

			for k, v := range req1Headers {
				req1.Header.Set(k, v)
			}

			signature := auth.SignRequest(
				client.AccessKey.AccessKeyID,
				client.AccessKey.AccessKeySecret,
				req1.Method,
				req1.URL.Path,
				req1Headers,
				nil,
				map[string]string{},
			)

			req1.Header.Set("Authorization", fmt.Sprintf("Litebase-HMAC-SHA256 %s", signature))

			// Make first request in goroutine
			resp1Chan := make(chan *http.Response, 1)
			err1Chan := make(chan error, 1)

			go func() {
				client := &http.Client{}
				resp, err := client.Do(req1)

				if err != nil {
					err1Chan <- err
					return
				}

				resp1Chan <- resp
			}()

			// Wait for first export to start
			var resp1 *http.Response
			select {
			case resp1 = <-resp1Chan:
				// Check if the response is an error
				if resp1.StatusCode != http.StatusOK {
					body, err := io.ReadAll(resp1.Body)

					if err != nil {
						t.Fatal(err)
					}

					t.Fatalf("First export failed with status %d: %s", resp1.StatusCode, string(body))
				}

				defer func() {
					err := resp1.Body.Close()

					if err != nil {
						t.Fatal(err)
					}
				}()

				// Read just the JSON response (first line)
				var exportData map[string]any

				decoder := json.NewDecoder(resp1.Body)

				if err := decoder.Decode(&exportData); err != nil {
					t.Fatal(err)
				}

				// Don't read anymore - the connection will stay open with keepalive
				// The server will detect disconnect when we close the body
				time.Sleep(1 * time.Second)

			case err := <-err1Chan:
				t.Fatalf("First export request failed: %v", err)
				t.Fatal("Timeout waiting for first export")
			}

			// Try to start second export (should fail)
			req2, err := http.NewRequest("POST", url, nil)

			if err != nil {
				t.Fatal(err)
			}

			req2Headers := map[string]string{
				"Host":            req2.URL.Host,
				"Content-Type":    "application/json",
				"X-Litebase-Date": fmt.Sprintf("%d", time.Now().UTC().Unix()),
			}

			for k, v := range req2Headers {
				req2.Header.Set(k, v)
			}

			signature = auth.SignRequest(
				client.AccessKey.AccessKeyID,
				client.AccessKey.AccessKeySecret,
				req2.Method,
				req2.URL.Path,
				req2Headers,
				nil,
				map[string]string{},
			)

			req2.Header.Set("Authorization", fmt.Sprintf("Litebase-HMAC-SHA256 %s", signature))

			httpClient := &http.Client{
				Timeout: 2 * time.Second,
			}

			resp2, err := httpClient.Do(req2)

			if err != nil {
				// This is expected - connection should fail or timeout
				return
			}

			defer func() {
				err := resp2.Body.Close()

				if err != nil {
					t.Fatal(err)
				}
			}()

			// If we get a response, it should be an error
			if resp2.StatusCode != http.StatusConflict && resp2.StatusCode != http.StatusInternalServerError {
				body, err := io.ReadAll(resp2.Body)

				if err != nil {
					t.Fatal(err)
				}

				t.Fatalf("Expected conflict or error status, got %d: %s", resp2.StatusCode, string(body))
			}

			// Close first export connection to clean up
			err = resp1.Body.Close()

			if err != nil {
				t.Fatal(err)
			}
		})

		t.Run("DatabaseExportControllerStore_Unauthorized", func(t *testing.T) {
			url := fmt.Sprintf(
				"%s/v1/databases/%s/branches/%s/export",
				testServer.Server.URL,
				testDatabase.DatabaseName,
				testDatabase.BranchName,
			)

			req, err := http.NewRequest("POST", url, nil)

			if err != nil {
				t.Fatal(err)
			}

			req.Header.Set("Content-Type", "application/json")

			// Don't set auth credentials

			httpClient := &http.Client{}
			resp, err := httpClient.Do(req)

			if err != nil {
				t.Fatal(err)
			}

			defer func() {
				err := resp.Body.Close()

				if err != nil {
					t.Fatal(err)
				}
			}()

			if resp.StatusCode != http.StatusUnauthorized && resp.StatusCode != http.StatusForbidden {
				t.Fatalf("Expected unauthorized or forbidden status, got %d", resp.StatusCode)
			}
		})
	})
}
