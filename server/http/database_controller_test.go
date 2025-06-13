package http_test

import (
	"fmt"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/server/auth"
)

func TestDatabaseControllerIndex(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		for range 3 {
			test.MockDatabase(server.App)
		}

		client := server.WithAccessKeyClient([]auth.AccessKeyStatement{{
			Effect:   "Allow",
			Resource: "*",
			Actions:  []auth.Privilege{auth.DatabasePrivilegeList},
		}})

		resp, statusCode, err := client.Send("/databases", "GET", nil)

		if err != nil {
			t.Fatalf("failed to send request: %v", err)
		}

		if statusCode != 200 {
			t.Fatalf("failed to get databases expected status code 200, got %d", statusCode)
		}

		if resp == nil {
			t.Fatalf("response is nil")
		}

		if resp["status"] != "success" {
			t.Fatalf("expected success status, got %v", resp["status"])
		}

		data, ok := resp["data"].([]any)

		if !ok {
			t.Fatalf("expected data to be an array, got %T", resp["data"])
		}

		if len(data) < 3 {
			t.Fatalf("expected at least 3 databases, got %d", len(data))
		}

	})
}

func TestDatabaseControllerShow(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		database := test.MockDatabase(server.App)

		client := server.WithAccessKeyClient([]auth.AccessKeyStatement{{
			Effect:   "Allow",
			Resource: "*",
			Actions:  []auth.Privilege{auth.DatabasePrivilegeShow},
		}})

		resp, statusCode, err := client.Send(fmt.Sprintf("/databases/%s", database.DatabaseId), "GET", nil)

		if err != nil {
			t.Fatalf("failed to send request: %v", err)
		}

		if statusCode != 200 {
			t.Fatalf("failed to get database expected status code 200, got %d", statusCode)
		}

		if resp == nil {
			t.Fatalf("response is nil")
		}

		if resp["status"] != "success" {
			t.Fatalf("expected success status, got %v", resp["status"])
		}

		data, ok := resp["data"].(map[string]any)

		if !ok {
			t.Fatalf("expected data to be an object, got %T", resp["data"])
		}

		if data["id"] != database.DatabaseId {
			t.Fatalf("expected database id to be %s, got %v", database.DatabaseId, data["id"])
		}

	})
}

func TestDatabaseControllerStore(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		client := server.WithAccessKeyClient([]auth.AccessKeyStatement{{
			Effect:   "Allow",
			Resource: "*",
			Actions:  []auth.Privilege{auth.DatabasePrivilegeCreate},
		}})

		resp, statusCode, err := client.Send("/databases", "POST", map[string]any{
			"name": "test_db",
		})

		if err != nil {
			t.Fatalf("failed to send request: %v", err)
		}
		if statusCode != 200 {
			t.Fatalf("expected status code 200, got %d", statusCode)
		}

		// Check the response data
		data, ok := resp["data"].(map[string]any)

		if !ok {
			t.Fatalf("expected data to be an object, got %T", resp["data"])
		}

		if data["name"] != "test_db" {
			t.Fatalf("expected database name to be 'test_db', got %v", data["name"])
		}

		database, err := server.App.DatabaseManager.Get(data["id"].(string))

		if err != nil {
			t.Fatalf("failed to get database: %v", err)
		}

		if database.Name != "test_db" {
			t.Fatalf("expected database name to be 'test_db', got %v", database.Name)
		}
	})
}

func TestDatabaseControllerStore_WithSameNameFails(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		mock := test.MockDatabase(server.App)

		database, err := server.App.DatabaseManager.Get(mock.DatabaseId)

		if err != nil {
			t.Fatalf("failed to get mock database: %v", err)
		}

		client := server.WithAccessKeyClient([]auth.AccessKeyStatement{{
			Effect:   "Allow",
			Resource: "*",
			Actions:  []auth.Privilege{auth.DatabasePrivilegeCreate},
		}})

		resp, statusCode, err := client.Send("/databases", "POST", map[string]any{
			"name": database.Name,
		})

		if err != nil {
			t.Fatalf("failed to send request: %v", err)
		}

		if statusCode != 400 {
			t.Fatalf("expected status code 400, got %d", statusCode)
		}

		if resp["status"] != "error" {
			t.Fatalf("expected error status, got %v", resp["status"])
		}

		databases, err := server.App.DatabaseManager.All()

		if err != nil {
			t.Fatalf("failed to get databases: %v", err)
		}

		if len(databases) != 1 {
			t.Fatalf("expected exactly 1 database, got %d", len(databases))
		}
	})
}

func TestDatabaseControllerDestroy(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		mock := test.MockDatabase(server.App)

		client := server.WithAccessKeyClient([]auth.AccessKeyStatement{{
			Effect:   "Allow",
			Resource: "*",
			Actions:  []auth.Privilege{auth.DatabasePrivilegeManage},
		}})

		resp, statusCode, err := client.Send(fmt.Sprintf("/databases/%s", mock.DatabaseId), "DELETE", nil)

		if err != nil {
			t.Fatalf("failed to send request: %v", err)
		}

		if statusCode != 200 {
			t.Fatalf("expected status code 200, got %d", statusCode)
		}

		if resp == nil {
			t.Fatalf("response is nil")
		}

		if resp["status"] != "success" {
			t.Fatalf("expected success status, got %v", resp["status"])
		}

		if resp["message"] != "Database deleted successfully." {
			t.Fatalf("expected message to be 'Database deleted successfully.', got %v", resp["message"])
		}
	})
}
