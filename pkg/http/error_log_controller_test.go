package http_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
)

func TestErrorLogControllerSuccess(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		mock := test.MockDatabase(server.App)

		// Get the error log and manually write some error entries
		errorLog := server.App.LogManager.GetErrorLog(
			server.App.Cluster,
			mock.DatabaseKey.DatabaseHash,
			mock.DatabaseID,
			mock.DatabaseBranchID,
		)

		// Write some test error entries
		for i := range 5 {
			err := errorLog.Write(
				mock.Credential.CredentialID,
				fmt.Sprintf("SELECT * FROM test%d", i),
				fmt.Sprintf("no such table: test%d", i),
				0.01,
			)

			if err != nil {
				t.Fatalf("Failed to write error entry: %v", err)
			}
		}

		// Give some time for errors to be recorded
		time.Sleep(100 * time.Millisecond)

		// Manually flush the error log to ensure entries are written
		errorLog.Flush(true)

		// Set up test time range
		now := time.Now()
		start := now.Add(-1 * time.Hour).Unix()
		end := now.Unix()

		client := server.WithAccessKeyClient([]auth.Statement{
			{
				Effect:   auth.StatementEffectAllow,
				Resource: "*",
				Actions:  []auth.Privilege{auth.DatabaseBranchPrivilegeShow},
			},
		})

		resp, responseCode, err := client.Send(
			fmt.Sprintf(
				"/v1/databases/%s/branches/%s/errors?start=%d&end=%d",
				mock.DatabaseName,
				mock.BranchName,
				start,
				end,
			),
			"GET",
			nil,
		)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if responseCode != 200 {
			t.Fatalf("Expected response code 200, got %d. Response: %v", responseCode, resp)
		}

		// Verify response structure
		if resp["status"] != "success" {
			t.Errorf("Expected status 'success', got %v", resp["status"])
		}

		// Check for data section
		data, ok := resp["data"].([]any)

		if !ok {
			t.Fatalf("Expected data to be an array, got %T", resp["data"])
		}

		// We should have 5 error entries from our manual writes
		if len(data) != 5 {
			t.Errorf("Expected 5 error entries, got %d", len(data))
		}

		// Verify structure of first entry
		if len(data) > 0 {
			entry, ok := data[0].(map[string]any)

			if !ok {
				t.Fatalf("Expected entry to be a map, got %T", data[0])
			}

			// Check for required fields
			if _, ok := entry["Timestamp"]; !ok {
				t.Error("Expected entry to have Timestamp field")
			}

			if _, ok := entry["CredentialID"]; !ok {
				t.Error("Expected entry to have CredentialID field")
			}

			if _, ok := entry["Statement"]; !ok {
				t.Error("Expected entry to have Statement field")
			}

			if _, ok := entry["Error"]; !ok {
				t.Error("Expected entry to have Error field")
			}

			if _, ok := entry["Latency"]; !ok {
				t.Error("Expected entry to have Latency field")
			}
		}

		t.Logf("Retrieved %d error entries", len(data))
	})
}

func TestErrorLogControllerInvalidParameters(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		mock := test.MockDatabase(server.App)

		client := server.WithAccessKeyClient([]auth.Statement{
			{
				Effect:   auth.StatementEffectAllow,
				Resource: "*",
				Actions:  []auth.Privilege{auth.DatabaseBranchPrivilegeShow},
			},
		})

		testCases := []struct {
			name         string
			queryParams  string
			expectedCode int
		}{
			{
				name:         "End before start",
				queryParams:  "?start=2000&end=1000",
				expectedCode: 400,
			},
			{
				name:         "Invalid start timestamp",
				queryParams:  "?start=9999999999999999999&end=2000",
				expectedCode: 400,
			},
			{
				name:         "Invalid end timestamp",
				queryParams:  "?start=1000&end=9999999999999999999",
				expectedCode: 400,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				resp, responseCode, err := client.Send(
					fmt.Sprintf(
						"/v1/databases/%s/branches/%s/errors%s",
						mock.DatabaseName,
						mock.BranchName,
						tc.queryParams,
					),
					"GET",
					nil,
				)

				if err != nil {
					t.Fatalf("Expected no error, got %v", err)
				}

				if responseCode != tc.expectedCode {
					t.Errorf("Expected response code %d, got %d. Response: %v", tc.expectedCode, responseCode, resp)
				}
			})
		}
	})
}

func TestErrorLogControllerUnauthorized(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		mock := test.MockDatabase(server.App)

		// Client with no permissions
		client := server.WithAccessKeyClient([]auth.Statement{
			{
				Effect:   auth.StatementEffectDeny,
				Resource: "*",
				Actions:  []auth.Privilege{auth.DatabaseBranchPrivilegeShow},
			},
		})

		now := time.Now()
		start := now.Add(-1 * time.Hour).Unix()
		end := now.Unix()

		resp, responseCode, err := client.Send(
			fmt.Sprintf(
				"/v1/databases/%s/branches/%s/errors?start=%d&end=%d",
				mock.DatabaseName,
				mock.BranchName,
				start,
				end,
			),
			"GET",
			nil,
		)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if responseCode != 403 {
			t.Errorf("Expected response code 403, got %d. Response: %v", responseCode, resp)
		}
	})
}

func TestErrorLogControllerNonExistentDatabase(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		client := server.WithAccessKeyClient([]auth.Statement{
			{
				Effect:   auth.StatementEffectAllow,
				Resource: "*",
				Actions:  []auth.Privilege{auth.DatabaseBranchPrivilegeShow},
			},
		})

		now := time.Now()
		start := now.Add(-1 * time.Hour).Unix()
		end := now.Unix()

		resp, responseCode, err := client.Send(
			fmt.Sprintf(
				"/v1/databases/%s/branches/%s/errors?start=%d&end=%d",
				"nonexistent",
				"main",
				start,
				end,
			),
			"GET",
			nil,
		)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if responseCode != 404 {
			t.Errorf("Expected response code 404, got %d. Response: %v", responseCode, resp)
		}
	})
}

func TestErrorLogControllerDefaultTimeRange(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		mock := test.MockDatabase(server.App)

		client := server.WithAccessKeyClient([]auth.Statement{
			{
				Effect:   auth.StatementEffectAllow,
				Resource: "*",
				Actions:  []auth.Privilege{auth.DatabaseBranchPrivilegeShow},
			},
		})

		// Request without time range parameters - should use defaults
		resp, responseCode, err := client.Send(
			fmt.Sprintf(
				"/v1/databases/%s/branches/%s/errors",
				mock.DatabaseName,
				mock.BranchName,
			),
			"GET",
			nil,
		)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if responseCode != 200 {
			t.Fatalf("Expected response code 200, got %d. Response: %v", responseCode, resp)
		}

		// Verify response structure
		if resp["status"] != "success" {
			t.Errorf("Expected status 'success', got %v", resp["status"])
		}
	})
}
