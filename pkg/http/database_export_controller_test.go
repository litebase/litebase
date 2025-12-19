package http_test

import (
	"fmt"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
)

func TestDatabaseExportController(t *testing.T) {
	test.Run(t, func() {
		testServer := test.NewTestServer(t)
		defer testServer.Shutdown()

		testDatabase := test.MockDatabase(testServer.App)

		// Clear any existing exports to ensure clean state
		exportManager, err := testServer.App.DatabaseManager.Resources(
			testDatabase.DatabaseID,
			testDatabase.DatabaseBranchID,
		).ExportManager()

		if err == nil {
			exportManager.Clear()
		}

		client := testServer.WithAccessKeyClient([]auth.Statement{
			{
				Effect:   auth.StatementEffectAllow,
				Resource: "*",
				Actions:  []auth.Privilege{auth.DatabasePrivilegeExport},
			},
		})

		t.Run("DatabaseExportControllerStore", func(t *testing.T) {
			response, statusCode, err := client.Send(
				fmt.Sprintf("/v1/databases/%s/branches/%s/export", testDatabase.DatabaseName, testDatabase.BranchName),
				"POST",
				nil,
			)

			if err != nil {
				t.Fatal(err)
			}

			if statusCode != 201 {
				t.Fatalf("Expected status code 201, got %d: %v", statusCode, response)
			}

			if response["status"] != "success" {
				t.Fatalf("Expected status 'success', got %s", response["status"])
			}

			data, ok := response["data"].(map[string]any)

			if !ok {
				t.Fatal("Expected response to have 'data' field")
			}

			if data["id"] == nil {
				t.Fatal("Expected export response to have 'id' field")
			}

			if data["rangeCount"] == nil {
				t.Fatal("Expected export response to have 'rangeCount' field")
			}

			if data["startedAt"] == nil {
				t.Fatal("Expected export response to have 'startedAt' field")
			}

			if data["expiresAt"] == nil {
				t.Fatal("Expected export response to have 'expiresAt' field")
			}

			rangeCount, ok := data["rangeCount"].(float64)

			if !ok {
				t.Fatal("Expected rangeCount to be a number")
			}

			if rangeCount == 0 {
				t.Fatal("Expected rangeCount to be greater than 0")
			}

			// Cleanup - end the export session
			exportID := data["id"].(string)

			_, endStatusCode, err := client.Send(
				fmt.Sprintf(
					"/v1/databases/%s/branches/%s/export/%s/end",
					testDatabase.DatabaseName,
					testDatabase.BranchName,
					exportID,
				),
				"POST",
				nil,
			)

			if err != nil {
				t.Fatal(err)
			}

			if endStatusCode != 204 {
				t.Fatalf("Expected end export status 204, got %d", endStatusCode)
			}
		})

		t.Run("DatabaseExportControllerStore_OnlyOneExportAllowed", func(t *testing.T) {
			url := fmt.Sprintf("/v1/databases/%s/branches/%s/export", testDatabase.DatabaseName, testDatabase.BranchName)

			// Start first export
			response1, statusCode1, err := client.Send(url, "POST", nil)

			if err != nil {
				t.Fatal(err)
			}

			if statusCode1 != 201 {
				t.Fatalf("First export failed with status %d", statusCode1)
			}

			data, ok := response1["data"].(map[string]any)

			if !ok {
				t.Fatal("Expected response to have 'data' field")
			}

			exportID := data["id"].(string)

			// Ensure cleanup happens
			defer func() {
				client.Send(
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

			// Try to start second export (should fail)
			response2, statusCode2, err := client.Send(url, "POST", nil)

			if err != nil {
				t.Fatal(err)
			}

			if statusCode2 != 409 {
				t.Fatalf("Expected conflict status 409, got %d: %v", statusCode2, response2)
			}

			if response2["status"] != "error" {
				t.Fatalf("Expected status 'error', got %s", response2["status"])
			}
		})
	})
}
