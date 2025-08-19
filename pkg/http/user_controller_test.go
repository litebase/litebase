package http_test

import (
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
)

func TestUserController(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		t.Run("Index", func(t *testing.T) {
			client := server.WithAccessKeyClient([]auth.Statement{
				{
					Effect:   "Allow",
					Resource: "*",
					Actions:  []auth.Privilege{auth.ClusterPrivilegeManage},
				},
			})

			response, statusCode, err := client.Send(
				"/v1/users",
				"GET", nil,
			)

			if err != nil {
				t.Fatalf("Failed to list users: %v", err)
			}

			if statusCode != 200 {
				t.Fatalf("Expected status code 200, got %d", statusCode)
			}

			if _, ok := response["data"]; !ok {
				t.Fatal("Response does not contain 'data' field")
			}

			users, ok := response["data"].([]any)

			if !ok {
				t.Fatal("Response 'data' field is not an array")
			}

			if len(users) == 0 {
				t.Fatal("Expected at least one user, got none")
			}
		})

		t.Run("Create", func(t *testing.T) {
			_, err := server.App.Cluster.Auth.UserManager.Create(
				"testuser",
				"password123",
				"",
				[]auth.Statement{
					{
						Effect:   "Allow",
						Resource: "*",
						Actions:  []auth.Privilege{auth.ClusterPrivilegeManage},
					},
				},
			)

			if err != nil {
				t.Fatalf("Failed to create test user: %v", err)
			}

			client := server.WithAccessKeyClient([]auth.Statement{
				{
					Effect:   "Allow",
					Resource: "*",
					Actions:  []auth.Privilege{auth.ClusterPrivilegeManage},
				},
			})

			response, statusCode, err := client.Send(
				"/v1/users/testuser",
				"GET", nil,
			)

			if err != nil {
				t.Fatalf("Failed to get user: %v", err)
			}

			if statusCode != 200 {
				t.Fatalf("Expected status code 200, got %d", statusCode)
			}

			if _, ok := response["data"]; !ok {
				t.Fatal("Response does not contain 'data' field")
			}

			user, ok := response["data"].(map[string]any)

			if !ok {
				t.Fatal("Response 'data' field is not an object")
			}

			if user["username"] != "testuser" {
				t.Fatalf("Expected username 'testuser', got '%s'", user["username"])
			}
		})

		t.Run("Store", func(t *testing.T) {
			client := server.WithAccessKeyClient([]auth.Statement{
				{
					Effect:   "Allow",
					Resource: "*",
					Actions:  []auth.Privilege{auth.ClusterPrivilegeManage},
				},
			})

			response, statusCode, err := client.Send(
				"/v1/users",
				"POST", map[string]any{
					"username": "testuser_store",
					"password": "password123",
					"statements": []auth.Statement{
						{
							Effect:   "Allow",
							Resource: "*",
							Actions:  []auth.Privilege{auth.ClusterPrivilegeManage},
						},
					},
				},
			)

			if err != nil {
				t.Fatalf("Failed to create user: %v", err)
			}

			if statusCode != 201 {
				t.Fatalf("Expected status code 201, got %d", statusCode)
			}

			if _, ok := response["data"]; !ok {
				t.Fatal("Response does not contain 'data' field")
			}
		})

		t.Run("StoreWithInvalidData", func(t *testing.T) {
			client := server.WithAccessKeyClient([]auth.Statement{
				{
					Effect:   "Allow",
					Resource: "*",
					Actions:  []auth.Privilege{auth.ClusterPrivilegeManage},
				},
			})

			response, statusCode, err := client.Send(
				"/v1/users",
				"POST", map[string]any{
					"username": "testuser",
					"password": "abc213",
					"statements": []auth.Statement{
						{
							Effect:   "Allow",
							Resource: "*",
							Actions:  []auth.Privilege{auth.ClusterPrivilegeManage},
						},
					},
				},
			)

			if err != nil {
				t.Fatalf("Failed to create user: %v", err)
			}

			if statusCode != 422 {
				t.Fatalf("Expected status code 422, got %d", statusCode)
			}

			if _, ok := response["errors"]; !ok {
				t.Fatal("Response does not contain 'errors' field")
			}
		})

		t.Run("Update", func(t *testing.T) {
			_, err := server.App.Cluster.Auth.UserManager.Create(
				"foo_update",
				"bar",
				"",
				[]auth.Statement{
					{
						Effect:   "Allow",
						Resource: "*",
						Actions:  []auth.Privilege{auth.ClusterPrivilegeManage},
					},
				},
			)

			if err != nil {
				t.Fatalf("Failed to create test user: %v", err)
			}

			client := server.WithAccessKeyClient([]auth.Statement{
				{
					Effect:   "Allow",
					Resource: "*",
					Actions:  []auth.Privilege{auth.ClusterPrivilegeManage},
				},
			})

			response, statusCode, err := client.Send(
				"/v1/users/foo_update",
				"PUT", map[string]any{
					"password": "newpassword123",
					"statements": []auth.Statement{
						{
							Effect:   "Deny",
							Resource: "*",
							Actions:  []auth.Privilege{auth.ClusterPrivilegeManage},
						},
					},
				},
			)

			if err != nil {
				t.Fatalf("Failed to update user: %v", err)
			}

			if statusCode != 200 {
				t.Fatalf("Expected status code 200, got %d", statusCode)
			}

			if _, ok := response["data"]; !ok {
				t.Fatal("Response does not contain 'data' field")
			}

			user, ok := response["data"].(map[string]any)

			if !ok {
				t.Fatal("Response 'data' field is not an object")
			}

			if user["username"] != "foo_update" {
				t.Fatalf("Expected username 'foo_update', got '%s'", user["username"])
			}

			if statements, ok := user["statements"].([]any); !ok || len(statements) != 1 {
				t.Fatal("User statements were not updated correctly")
			} else {
				statement, ok := statements[0].(map[string]any)

				if !ok || statement["effect"] != "Deny" {
					t.Fatal("User statements were not updated correctly")
				}
			}
		})

		t.Run("Destroy", func(t *testing.T) {
			_, err := server.App.Cluster.Auth.UserManager.Create(
				"foo_destroy",
				"bar",
				"",
				[]auth.Statement{
					{
						Effect:   "Allow",
						Resource: "*",
						Actions:  []auth.Privilege{auth.ClusterPrivilegeManage},
					},
				},
			)

			if err != nil {
				t.Fatalf("Failed to create test user: %v", err)
			}

			client := server.WithAccessKeyClient([]auth.Statement{
				{
					Effect:   "Allow",
					Resource: "*",
					Actions:  []auth.Privilege{auth.ClusterPrivilegeManage},
				},
			})

			response, statusCode, err := client.Send(
				"/v1/users/foo_destroy",
				"DELETE", nil,
			)

			if err != nil {
				t.Fatalf("Failed to delete user: %v", err)
			}

			if statusCode != 204 {
				t.Fatalf("Expected status code 204, got %d", statusCode)
			}

			if response != nil {
				t.Fatal("Expected no response body for DELETE request")
			}
		})
	})
}
