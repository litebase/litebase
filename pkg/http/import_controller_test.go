package http_test

import (
	"fmt"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
)

func TestImportControllerStore(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		t.Run("CreateImport", func(t *testing.T) {
			client := server.WithAccessKeyClient([]auth.Statement{{
				Effect:   auth.StatementEffectAllow,
				Resource: "*",
				Actions:  []auth.Privilege{auth.DatabasePrivilegeCreate, auth.DatabasePrivilegeImport},
			}})

			resp, statusCode, err := client.Send("/v1/imports", "POST", map[string]any{
				"databaseName": "test_import_db",
				"branchName":   "main",
				"chunkCount":   10,
			})

			if err != nil {
				t.Fatalf("failed to send request: %v", err)
			}

			if statusCode != 201 {
				t.Fatalf("expected status code 201, got %d: %v", statusCode, resp)
			}

			data, ok := resp["data"].(map[string]any)
			if !ok {
				t.Fatal("expected data object in response")
			}

			if _, ok := data["importId"]; !ok {
				t.Error("expected importId in response")
			}

			if _, ok := data["status"]; !ok {
				t.Error("expected status in response")
			}

			if _, ok := data["chunkCount"]; !ok {
				t.Error("expected chunkCount in response")
			}

			if data["status"] != "pending" {
				t.Errorf("expected status 'pending', got %v", data["status"])
			}
		})

		t.Run("InvalidChunkCount", func(t *testing.T) {
			client := server.WithAccessKeyClient([]auth.Statement{{
				Effect:   auth.StatementEffectAllow,
				Resource: "*",
				Actions:  []auth.Privilege{auth.DatabasePrivilegeCreate, auth.DatabasePrivilegeImport},
			}})

			resp, statusCode, err := client.Send("/v1/imports", "POST", map[string]any{
				"databaseName": "test_db",
				"branchName":   "main",
				"chunkCount":   0,
			})

			if err != nil {
				t.Fatalf("failed to send request: %v", err)
			}

			if statusCode != 422 {
				t.Fatalf("expected status code 422, got %d: %v", statusCode, resp)
			}
		})
	})
}

func TestImportControllerShow(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		t.Run("GetImport", func(t *testing.T) {
			client := server.WithAccessKeyClient([]auth.Statement{{
				Effect:   auth.StatementEffectAllow,
				Resource: "*",
				Actions:  []auth.Privilege{auth.DatabasePrivilegeCreate, auth.DatabasePrivilegeImport},
			}})

			// Create import first
			createResp, statusCode, err := client.Send("/v1/imports", "POST", map[string]any{
				"databaseName": "test_show_db",
				"branchName":   "main",
				"chunkCount":   5,
			})

			if err != nil {
				t.Fatalf("failed to create import: %v", err)
			}

			if statusCode != 201 {
				t.Fatalf("expected status code 201, got %d: %v", statusCode, createResp)
			}

			createData, ok := createResp["data"].(map[string]any)
			if !ok {
				t.Fatal("expected data object in create response")
			}

			importID := createData["importId"]

			// Get import
			getResp, statusCode, err := client.Send(fmt.Sprintf("/v1/imports/%v", importID), "GET", nil)

			if err != nil {
				t.Fatalf("failed to get import: %v", err)
			}

			if statusCode != 200 {
				t.Fatalf("expected status code 200, got %d: %v", statusCode, getResp)
			}

			getData, ok := getResp["data"].(map[string]any)
			if !ok {
				t.Fatal("expected data object in get response")
			}

			if _, ok := getData["status"]; !ok {
				t.Error("expected status in response")
			}

			if _, ok := getData["uploadedChunks"]; !ok {
				t.Error("expected uploadedChunks in response")
			}

			if _, ok := getData["missingChunks"]; !ok {
				t.Error("expected missingChunks in response")
			}
		})

		t.Run("NotFound", func(t *testing.T) {
			client := server.WithAccessKeyClient([]auth.Statement{{
				Effect:   auth.StatementEffectAllow,
				Resource: "*",
				Actions:  []auth.Privilege{auth.DatabasePrivilegeImport},
			}})

			resp, statusCode, err := client.Send("/v1/imports/99999", "GET", nil)

			if err != nil {
				t.Fatalf("failed to send request: %v", err)
			}

			if statusCode != 404 {
				t.Fatalf("expected status code 404, got %d: %v", statusCode, resp)
			}
		})

		t.Run("InvalidID", func(t *testing.T) {
			client := server.WithAccessKeyClient([]auth.Statement{{
				Effect:   auth.StatementEffectAllow,
				Resource: "*",
				Actions:  []auth.Privilege{auth.DatabasePrivilegeImport},
			}})

			resp, statusCode, err := client.Send("/v1/imports/invalid", "GET", nil)

			if err != nil {
				t.Fatalf("failed to send request: %v", err)
			}

			if statusCode != 400 {
				t.Fatalf("expected status code 400, got %d: %v", statusCode, resp)
			}
		})
	})
}

func TestImportControllerDestroy(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		t.Run("DeleteImport", func(t *testing.T) {
			client := server.WithAccessKeyClient([]auth.Statement{{
				Effect:   auth.StatementEffectAllow,
				Resource: "*",
				Actions:  []auth.Privilege{auth.DatabasePrivilegeCreate, auth.DatabasePrivilegeImport, auth.DatabasePrivilegeDelete},
			}})

			// Create import first
			createResp, statusCode, err := client.Send("/v1/imports", "POST", map[string]any{
				"databaseName": "test_delete_db",
				"branchName":   "main",
				"chunkCount":   3,
			})

			if err != nil {
				t.Fatalf("failed to create import: %v", err)
			}

			if statusCode != 201 {
				t.Fatalf("expected status code 201, got %d: %v", statusCode, createResp)
			}

			createData, ok := createResp["data"].(map[string]any)
			if !ok {
				t.Fatal("expected data object in create response")
			}

			importID := createData["importId"]

			// Delete import
			deleteResp, statusCode, err := client.Send(fmt.Sprintf("/v1/imports/%v", importID), "DELETE", nil)

			if err != nil {
				t.Fatalf("failed to delete import: %v", err)
			}

			if statusCode != 200 {
				t.Fatalf("expected status code 200, got %d: %v", statusCode, deleteResp)
			}

			// Verify it's deleted by trying to get it
			getResp, statusCode, err := client.Send(fmt.Sprintf("/v1/imports/%v", importID), "GET", nil)

			if err != nil {
				t.Fatalf("failed to get import after delete: %v", err)
			}

			if statusCode != 404 {
				t.Fatalf("expected status code 404 after delete, got %d: %v", statusCode, getResp)
			}
		})

		t.Run("NotFound", func(t *testing.T) {
			client := server.WithAccessKeyClient([]auth.Statement{{
				Effect:   auth.StatementEffectAllow,
				Resource: "*",
				Actions:  []auth.Privilege{auth.DatabasePrivilegeDelete, auth.DatabasePrivilegeImport},
			}})

			resp, statusCode, err := client.Send("/v1/imports/99999", "DELETE", nil)

			if err != nil {
				t.Fatalf("failed to send request: %v", err)
			}

			if statusCode != 404 {
				t.Fatalf("expected status code 404, got %d: %v", statusCode, resp)
			}
		})
	})
}

func TestImportControllerIntegration(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		t.Run("CompleteImportFlow", func(t *testing.T) {
			client := server.WithAccessKeyClient([]auth.Statement{{
				Effect:   auth.StatementEffectAllow,
				Resource: "*",
				Actions:  []auth.Privilege{auth.DatabasePrivilegeCreate, auth.DatabasePrivilegeImport, auth.DatabasePrivilegeDelete},
			}})

			// Create import
			createResp, statusCode, err := client.Send("/v1/imports", "POST", map[string]any{
				"databaseName": "test_flow_db",
				"branchName":   "main",
				"chunkCount":   2,
			})

			if err != nil {
				t.Fatalf("failed to create import: %v", err)
			}

			if statusCode != 201 {
				t.Fatalf("expected status code 201, got %d: %v", statusCode, createResp)
			}

			createData, ok := createResp["data"].(map[string]any)
			if !ok {
				t.Fatal("expected data object in create response")
			}

			importID := createData["importId"]

			// Check initial status
			getResp, statusCode, err := client.Send(fmt.Sprintf("/v1/imports/%v", importID), "GET", nil)

			if err != nil {
				t.Fatalf("failed to get import: %v", err)
			}

			if statusCode != 200 {
				t.Fatalf("expected status code 200, got %d: %v", statusCode, getResp)
			}

			getData, ok := getResp["data"].(map[string]any)
			if !ok {
				t.Fatal("expected data object in get response")
			}

			if getData["status"] != "pending" {
				t.Errorf("expected status 'pending', got %v", getData["status"])
			}

			// Delete import
			deleteResp, statusCode, err := client.Send(fmt.Sprintf("/v1/imports/%v", importID), "DELETE", nil)

			if err != nil {
				t.Fatalf("failed to delete import: %v", err)
			}

			if statusCode != 200 {
				t.Fatalf("expected status code 200, got %d: %v", statusCode, deleteResp)
			}
		})
	})
}

