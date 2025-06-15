package http_test

import (
	"fmt"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
)

func TestAccessKeyControllerDestroy(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		accessKey, err := server.App.Auth.AccessKeyManager.Create([]auth.AccessKeyStatement{
			{Effect: "Allow", Resource: "*", Actions: []auth.Privilege{"*"}},
		})

		if err != nil {
			t.Fatalf("Failed to create test access key: %v", err)
		}

		client := server.WithAccessKeyClient([]auth.AccessKeyStatement{
			{
				Effect:   "Allow",
				Resource: "*",
				Actions:  []auth.Privilege{"access-key:delete"},
			},
		})

		response, statusCode, err := client.Send(fmt.Sprintf("/access-keys/%s", accessKey.AccessKeyId), "DELETE", nil)

		if err != nil {
			t.Fatalf("Failed to delete access key: %v", err)
		}

		if statusCode != 200 {
			t.Fatalf("Unexpected status code: %d, expected 200", statusCode)
		}

		if response["status"] != "success" {
			t.Errorf("Unexpected response: %v", response)
		}

		if response["data"] != nil && (response["data"].(map[string]any)["access_key_id"] == nil || response["data"].(map[string]any)["access_key_secret"] == nil) {
			t.Errorf("Unexpected response: %v", response)
		}
	})
}

func TestAccessKeyControllerDestroy_CannotDeleteCurrentAccessKey(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Server.Close()

		client := server.WithAccessKeyClient([]auth.AccessKeyStatement{
			{
				Effect:   "Allow",
				Resource: "*",
				Actions:  []auth.Privilege{"access-key:delete"},
			},
		})

		response, statusCode, err := client.Send(fmt.Sprintf("/access-keys/%s", client.AccessKey.AccessKeyId), "DELETE", nil)

		if err != nil {
			t.Fatalf("Failed to delete access key: %v", err)
		}

		if statusCode != 403 {
			t.Fatalf("Unexpected status code: %d, expected 403", statusCode)
		}

		if response["status"] != "error" {
			t.Errorf("Unexpected response: %v", response)
		}
	})
}

func TestAccessKeyControllerDestroy_CannotDeleteWithInvalidAccessKey(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Server.Close()

		accessKey, err := server.App.Auth.AccessKeyManager.Create([]auth.AccessKeyStatement{
			{Effect: "Allow", Resource: "*", Actions: []auth.Privilege{"*"}},
		})

		if err != nil {
			t.Fatalf("Failed to create test access key: %v", err)
		}

		client := server.WithAccessKeyClient([]auth.AccessKeyStatement{
			{
				Effect:   "Allow",
				Resource: "access-key:foobar",
				Actions:  []auth.Privilege{"access-key:delete"},
			},
		})

		response, statusCode, err := client.Send(fmt.Sprintf("/access-keys/%s", accessKey.AccessKeyId), "DELETE", nil)

		if err != nil {
			t.Fatalf("Failed to delete access key: %v", err)
		}

		if statusCode != 403 {
			t.Fatalf("Unexpected status code: %d, expected 403", statusCode)
		}

		if response["status"] != "error" {
			t.Errorf("Unexpected response: %v", response)
		}
	})
}

func TestAccessKeyControllerIndex(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Server.Close()

		client := server.WithAccessKeyClient([]auth.AccessKeyStatement{
			{
				Effect:   "Allow",
				Resource: "*",
				Actions:  []auth.Privilege{"access-key:list"},
			},
		})

		response, statusCode, err := client.Send("/access-keys", "GET", nil)

		if err != nil {
			t.Fatalf("Failed to list access keys: %v", err)
		}

		if statusCode != 200 {
			t.Fatalf("Unexpected status code: %d, expected 200", statusCode)
		}

		if response["status"] != "success" {
			t.Errorf("Unexpected response: %v", response)
		}

		if response["data"] == nil || len(response["data"].([]any)) == 0 {
			t.Errorf("Expected at least one access key in response, got: %v", response["data"])
		}
	})
}

func TestAccessKeyControllerStore(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Server.Close()

		client := server.WithAccessKeyClient([]auth.AccessKeyStatement{
			{
				Effect:   "Allow",
				Resource: "*",
				Actions:  []auth.Privilege{"access-key:create"},
			},
		})

		response, statusCode, err := client.Send("/access-keys", "POST", map[string]any{
			"resource": "*",
			"statements": []map[string]any{
				{
					"effect":   "allow",
					"resource": "*",
					"actions":  []auth.Privilege{"*"},
				},
			},
		})

		if err != nil {
			t.Fatalf("Failed to create access key: %v", err)
		}

		if statusCode != 200 {
			t.Errorf("Unexpected status code: %d, expected 200", statusCode)
		}

		if response["status"] != "success" {
			t.Errorf("Unexpected response: %v", response)
		}

		if response["data"] != nil && (response["data"].(map[string]any)["access_key_id"] == nil || response["data"].(map[string]any)["access_key_secret"] == nil) {
			t.Errorf("Unexpected response: %v", response)
		}

		if response["errors"] != nil {
			t.Errorf("Expected no errors in response, got: %v", response["errors"])
		}
	})
}

func TestAccessKeyControllerStore_WithInvalidAccessKey(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Server.Close()

		client := server.WithAccessKeyClient([]auth.AccessKeyStatement{
			{
				Effect:   "Allow",
				Resource: "database:*",
				Actions:  []auth.Privilege{"access-key:create"},
			},
		})

		response, statusCode, err := client.Send("/access-keys", "POST", nil)

		if err != nil {
			t.Fatalf("Failed to create access key: %v", err)
		}

		if statusCode != 403 {
			t.Fatalf("Unexpected status code: %d, expected 403", statusCode)
		}

		if response["status"] != "error" {
			t.Errorf("Unexpected response: %v", response)
		}

		if response["data"] != nil {
			t.Errorf("Unexpected response: %v", response)
		}
	})
}

func TestAccessKeyControllerStore_WithInvalidInput(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Server.Close()

		client := server.WithAccessKeyClient([]auth.AccessKeyStatement{
			{
				Effect:   "Allow",
				Resource: "*",
				Actions:  []auth.Privilege{"access-key:create"},
			},
		})

		response, statusCode, err := client.Send("/access-keys", "POST", map[string]any{
			"resource":   "*",
			"statements": "",
		})

		if err != nil {
			t.Fatalf("Failed to create access key: %v", err)
		}

		if statusCode != 400 {
			t.Fatalf("Unexpected status code: %d, expected 400", statusCode)
		}

		if response["status"] != "error" {
			t.Errorf("Unexpected response: %v", response)
		}

		if response["data"] != nil {
			t.Errorf("Unexpected response: %v", response)
		}

		response, statusCode, err = client.Send("/access-keys", "POST", map[string]any{
			"resource":   "",
			"statements": []map[string]any{},
		})

		if err != nil {
			t.Fatalf("Failed to create access key: %v", err)
		}

		if statusCode != 422 {
			t.Fatalf("Unexpected status code: %d, expected 422", statusCode)
		}

		if response["status"] != "error" {
			t.Errorf("Unexpected response: %v", response)
		}

		if response["data"] != nil {
			t.Errorf("Unexpected response: %v", response)
		}

		if response["errors"] == nil || len(response["errors"].(map[string]any)) == 0 {
			t.Errorf("Expected errors in response, got: %v", response)
		}

		response, statusCode, err = client.Send("/access-keys", "POST", map[string]any{
			"statements": []map[string]any{
				{
					"effect":   "Allowed",
					"resource": "*",
					"actions":  []auth.Privilege{"*"},
				},
			},
		})

		if err != nil {
			t.Fatalf("Failed to create access key: %v", err)
		}

		if statusCode != 422 {
			t.Fatalf("Unexpected status code: %d, expected 422", statusCode)
		}

		if response["status"] != "error" {
			t.Errorf("Unexpected response: %v", response)
		}

		if response["data"] != nil {
			t.Errorf("Unexpected response: %v", response)
		}

		if response["errors"] == nil || len(response["errors"].(map[string]any)) == 0 {
			t.Errorf("Expected errors in response, got: %v", response)
		}
	})
}

func TestAccessKeyControllerStore_WithClusterUser(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Server.Close()

		client := server.WithBasicAuthClient()

		response, statusCode, err := client.Send("/access-keys", "POST", map[string]any{
			"resource": "*",
			"statements": []map[string]any{
				{
					"effect":   "allow",
					"resource": "*",
					"actions":  []auth.Privilege{"*"},
				},
			},
		})

		if err != nil {
			t.Fatalf("Failed to create access key: %v", err)
		}

		if statusCode != 200 {
			t.Errorf("Unexpected status code: %d, expected 200", statusCode)
		}

		if response["status"] != "success" {
			t.Errorf("Unexpected response: %v", response)
		}

		if response["data"] != nil && (response["data"].(map[string]any)["access_key_id"] == nil || response["data"].(map[string]any)["access_key_secret"] == nil) {
			t.Errorf("Unexpected response: %v", response)
		}

		if response["errors"] != nil {
			t.Errorf("Expected no errors in response, got: %v", response["errors"])
		}
	})
}

func TestAccessKeyControllerUpdate(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Server.Close()

		accessKey, err := server.App.Auth.AccessKeyManager.Create([]auth.AccessKeyStatement{
			{Effect: "Allow", Resource: "*", Actions: []auth.Privilege{"*"}},
		})

		if err != nil {
			t.Fatalf("Failed to create test access key: %v", err)
		}

		client := server.WithAccessKeyClient([]auth.AccessKeyStatement{
			{
				Effect:   "Allow",
				Resource: "*",
				Actions:  []auth.Privilege{"access-key:update"},
			},
		})

		response, statusCode, err := client.Send(fmt.Sprintf("/access-keys/%s", accessKey.AccessKeyId), "PUT", map[string]any{
			"statements": []map[string]any{
				{
					"effect":   "allow",
					"resource": "*",
					"actions":  []auth.Privilege{"*"},
				},
			},
		})

		if err != nil {
			t.Fatalf("Failed to update access key: %v", err)
		}

		if statusCode != 200 {
			t.Fatalf("Unexpected status code: %d, expected 200", statusCode)
		}

		if response["status"] != "success" {
			t.Errorf("Unexpected response: %v", response)
		}

		if response["data"] == nil || response["data"].(map[string]any)["access_key_id"] == nil || response["data"].(map[string]any)["statements"] == nil {
			t.Errorf("Unexpected response: %v", response)
		}
	})
}

func TestAccessKeyControllerUpdate_WithInvalidAccessKey(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Server.Close()

		accessKey, err := server.App.Auth.AccessKeyManager.Create([]auth.AccessKeyStatement{
			{Effect: "Allow", Resource: "*", Actions: []auth.Privilege{"*"}},
		})

		if err != nil {
			t.Fatalf("Failed to create test access key: %v", err)
		}

		client := server.WithAccessKeyClient([]auth.AccessKeyStatement{
			{
				Effect:   "Allow",
				Resource: "access-key:foobar",
				Actions:  []auth.Privilege{"access-key:update"},
			},
		})

		response, statusCode, err := client.Send(fmt.Sprintf("/access-keys/%s", accessKey.AccessKeyId), "PUT", map[string]any{
			"statements": []map[string]any{
				{
					"effect":   "allow",
					"resource": "*",
					"actions":  []auth.Privilege{"*"},
				},
			},
		})

		if err != nil {
			t.Fatalf("Failed to update access key: %v", err)
		}

		if statusCode != 403 {
			t.Fatalf("Unexpected status code: %d, expected 403", statusCode)
		}

		if response["status"] != "error" {
			t.Errorf("Unexpected response: %v", response)
		}
	})
}
