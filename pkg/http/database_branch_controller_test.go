package http_test

import (
	"fmt"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
)

func TestDatabaseBranchControllerIndex(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()
		testDatabase := test.MockDatabase(server.App)

		db, err := server.App.DatabaseManager.Get(testDatabase.DatabaseID)

		if err != nil {
			t.Fatalf("failed to get mock database: %v", err)
		}

		for i := range 3 {
			db.CreateBranch(fmt.Sprintf("branch-%d", i), "main")
		}

		client := server.WithAccessKeyClient([]auth.AccessKeyStatement{{
			Effect:   "Allow",
			Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s", db.DatabaseID)),
			Actions:  []auth.Privilege{auth.DatabaseBranchPrivilegeList},
		}})

		resp, statusCode, err := client.Send(fmt.Sprintf("/resources/databases/%s/branches", db.DatabaseID), "GET", nil)

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
			t.Fatalf("expected at least 3 database branches, got %d", len(data))
		}
	})
}

func TestDatabaseBranchControllerShow(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		database := test.MockDatabase(server.App)

		db, err := server.App.DatabaseManager.Get(database.DatabaseID)

		if err != nil {
			t.Fatalf("failed to get mock database: %v", err)
		}

		client := server.WithAccessKeyClient([]auth.AccessKeyStatement{{
			Effect:   "Allow",
			Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s:branch:*", db.DatabaseID)),
			Actions:  []auth.Privilege{auth.DatabasePrivilegeShow},
		}})

		resp, statusCode, err := client.Send(fmt.Sprintf("/resources/databases/%s/branches/%s", database.DatabaseID, db.PrimaryBranch().DatabaseBranchID), "GET", nil)

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

		if data["database_branch_id"] != db.PrimaryBranch().DatabaseBranchID {
			t.Fatalf("expected database branch id to be %s, got %v", db.PrimaryBranch().DatabaseBranchID, data["database_branch_id"])
		}
	})
}

func TestDatabaseBranchControllerStore(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		mock := test.MockDatabase(server.App)

		db, err := server.App.DatabaseManager.Get(mock.DatabaseID)

		if err != nil {
			t.Fatalf("failed to get mock database: %v", err)
		}

		client := server.WithAccessKeyClient([]auth.AccessKeyStatement{{
			Effect:   "Allow",
			Resource: auth.AccessKeyResource(fmt.Sprintf("database:%s", mock.DatabaseID)),
			Actions:  []auth.Privilege{auth.DatabaseBranchPrivilegeCreate},
		}})

		resp, statusCode, err := client.Send(fmt.Sprintf("/resources/databases/%s/branches", mock.DatabaseID), "POST", map[string]any{
			"name": "test_branch",
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

		if data["name"] != "test_branch" {
			t.Fatalf("expected database name to be 'test_branch', got %v", data["name"])
		}

		branch, err := db.Branch(data["database_branch_id"].(string))

		if err != nil {
			t.Fatalf("failed to get branch: %v", err)
		}

		if branch.Name != "test_branch" {
			t.Fatalf("expected branch name to be 'test_branch', got %v", branch.Name)
		}
	})
}

func TestDatabaseBranchControllerStore_WithSameNameFails(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		mock := test.MockDatabase(server.App)

		database, err := server.App.DatabaseManager.Get(mock.DatabaseID)

		if err != nil {
			t.Fatalf("failed to get mock database: %v", err)
		}

		client := server.WithAccessKeyClient([]auth.AccessKeyStatement{{
			Effect:   "Allow",
			Resource: "*",
			Actions:  []auth.Privilege{auth.DatabasePrivilegeCreate},
		}})

		resp, statusCode, err := client.Send("/resources/databases", "POST", map[string]any{
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

func TestDatabaseBranchControllerDestroy(t *testing.T) {
	test.Run(t, func() {
		t.Run("ExistingDatabase", func(t *testing.T) {
			server := test.NewTestServer(t)
			defer server.Shutdown()

			mock := test.MockDatabase(server.App)

			client := server.WithAccessKeyClient([]auth.AccessKeyStatement{{
				Effect:   "Allow",
				Resource: "*",
				Actions:  []auth.Privilege{auth.DatabasePrivilegeManage},
			}})

			resp, statusCode, err := client.Send(fmt.Sprintf("/resources/databases/%s", mock.DatabaseID), "DELETE", nil)

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

		t.Run("NonExistingDatabase", func(t *testing.T) {
			server := test.NewTestServer(t)
			defer server.Shutdown()

			client := server.WithAccessKeyClient([]auth.AccessKeyStatement{{
				Effect:   "Allow",
				Resource: "*",
				Actions:  []auth.Privilege{auth.DatabasePrivilegeManage},
			}})

			resp, statusCode, err := client.Send("/resources/databases/non-existing-id", "DELETE", nil)

			if err != nil {
				t.Fatalf("failed to send request: %v", err)
			}

			if statusCode != 400 {
				t.Fatalf("expected status code 400, got %d", statusCode)
			}

			if resp == nil {
				t.Fatalf("response is nil")
			}

			if resp["status"] != "error" {
				t.Fatalf("expected error status, got %v", resp["status"])
			}

			if resp["message"] != "Error: failed to find database" {
				t.Fatalf("expected message to be 'Error: failed to find database', got %v", resp["message"])
			}
		})
	})
}
