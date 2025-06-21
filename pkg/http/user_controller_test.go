package http_test

import (
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
)

func TestUserController_List(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		client := server.WithAccessKeyClient([]auth.AccessKeyStatement{
			{
				Effect:   "Allow",
				Resource: "*",
				Actions:  []auth.Privilege{auth.ClusterPrivilegeManage},
			},
		})

		response, statusCode, err := client.Send(
			"/resources/users",
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
}

func TestUserController_Store(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		client := server.WithAccessKeyClient([]auth.AccessKeyStatement{
			{
				Effect:   "Allow",
				Resource: "*",
				Actions:  []auth.Privilege{auth.ClusterPrivilegeManage},
			},
		})

		response, statusCode, err := client.Send(
			"/resources/users",
			"POST", map[string]any{
				"username": "testuser",
				"password": "password123",
				"statements": []auth.AccessKeyStatement{
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
}

func TestUserController_Destroy(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		err := server.App.Cluster.Auth.UserManager().Add(
			"foo",
			"bar",
			[]auth.AccessKeyStatement{
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

		client := server.WithAccessKeyClient([]auth.AccessKeyStatement{
			{
				Effect:   "Allow",
				Resource: "*",
				Actions:  []auth.Privilege{auth.ClusterPrivilegeManage},
			},
		})

		response, statusCode, err := client.Send(
			"/resources/users/foo",
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
}
