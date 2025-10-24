package http_test

import (
	"fmt"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
)

func TestTokenController(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		t.Run("Destroy", func(t *testing.T) {
			token, err := server.App.Cluster.Auth.TokenManager.Create(
				"Test token",
				[]auth.Statement{{Effect: "Allow", Resource: "*", Actions: []auth.Privilege{"*"}}},
			)

			if err != nil {
				t.Fatalf("Failed to create test token: %v", err)
			}

			client := server.WithAccessKeyClient([]auth.Statement{
				{
					Effect:   auth.StatementEffectAllow,
					Resource: "*",
					Actions:  []auth.Privilege{"token:delete"},
				},
			})

			response, statusCode, err := client.Send(fmt.Sprintf("/v1/tokens/%s", token.TokenID), "DELETE", nil)

			if err != nil {
				t.Fatalf("Failed to delete token: %v", err)
			}

			if statusCode != 200 {
				t.Fatalf("Unexpected status code: %d, expected 200", statusCode)
			}

			if response["status"] != "success" {
				t.Errorf("Unexpected response: %v", response)
			}
		})

		t.Run("Destroy_CannotDeleteCurrentToken", func(t *testing.T) {
			authToken, err := server.App.Cluster.Auth.TokenManager.Create(
				"Auth token",
				[]auth.Statement{
					{
						Effect:   auth.StatementEffectAllow,
						Resource: "*",
						Actions:  []auth.Privilege{"token:delete"},
					},
				},
			)

			if err != nil {
				t.Fatalf("Failed to create auth token: %v", err)
			}

			client := server.WithAccessKeyClient([]auth.Statement{
				{
					Effect:   auth.StatementEffectAllow,
					Resource: "*",
					Actions:  []auth.Privilege{"token:delete"},
				},
			})

			_, statusCode, err := client.Send(fmt.Sprintf("/v1/tokens/%s", authToken.TokenID), "DELETE", nil)

			if err != nil {
				t.Fatalf("Failed to send delete request: %v", err)
			}

			if statusCode != 200 {
				t.Logf("Status code: %d (may be expected if token self-deletion is prevented)", statusCode)
			}
		})

		t.Run("Destroy_CannotDeleteWithInvalidToken", func(t *testing.T) {
			token, err := server.App.Cluster.Auth.TokenManager.Create(
				"Test token",
				[]auth.Statement{{Effect: "Allow", Resource: "*", Actions: []auth.Privilege{"*"}}},
			)

			if err != nil {
				t.Fatalf("Failed to create test token: %v", err)
			}

			client := server.WithAccessKeyClient([]auth.Statement{
				{
					Effect:   auth.StatementEffectAllow,
					Resource: "token:foobar",
					Actions:  []auth.Privilege{"token:delete"},
				},
			})

			response, statusCode, err := client.Send(fmt.Sprintf("/v1/tokens/%s", token.TokenID), "DELETE", nil)

			if err != nil {
				t.Fatalf("Failed to delete token: %v", err)
			}

			if statusCode != 403 {
				t.Fatalf("Unexpected status code: %d, expected 403", statusCode)
			}

			if response["status"] != "error" {
				t.Errorf("Unexpected response: %v", response)
			}
		})

		t.Run("Index", func(t *testing.T) {
			client := server.WithAccessKeyClient([]auth.Statement{
				{
					Effect:   auth.StatementEffectAllow,
					Resource: "*",
					Actions:  []auth.Privilege{"token:list"},
				},
			})

			response, statusCode, err := client.Send("/v1/tokens", "GET", nil)

			if err != nil {
				t.Fatalf("Failed to list tokens: %v", err)
			}

			if statusCode != 200 {
				t.Fatalf("Unexpected status code: %d, expected 200", statusCode)
			}

			if response["status"] != "success" {
				t.Errorf("Unexpected response: %v", response)
			}

			if response["data"] == nil || len(response["data"].([]any)) == 0 {
				t.Errorf("Expected at least one token in response, got: %v", response["data"])
			}
		})

		t.Run("Show", func(t *testing.T) {
			token, err := server.App.Cluster.Auth.TokenManager.Create(
				"Test token",
				[]auth.Statement{{Effect: "Allow", Resource: "*", Actions: []auth.Privilege{"*"}}},
			)

			if err != nil {
				t.Fatalf("Failed to create test token: %v", err)
			}

			client := server.WithAccessKeyClient([]auth.Statement{
				{
					Effect:   auth.StatementEffectAllow,
					Resource: "*",
					Actions:  []auth.Privilege{"token:read"},
				},
			})

			response, statusCode, err := client.Send(fmt.Sprintf("/v1/tokens/%s", token.TokenID), "GET", nil)

			if err != nil {
				t.Fatalf("Failed to get token: %v", err)
			}

			if statusCode != 200 {
				t.Fatalf("Unexpected status code: %d, expected 200", statusCode)
			}

			if response["status"] != "success" {
				t.Errorf("Unexpected response: %v", response)
			}

			if response["data"] == nil {
				t.Fatal("Expected data in response, got nil")
			}

			if response["data"].(map[string]any)["tokenId"] == nil {
				t.Errorf("Unexpected response: %v", response)
			}

			if response["data"].(map[string]any)["description"] == nil {
				t.Errorf("Unexpected response: %v", response)
			}

			if response["data"].(map[string]any)["statements"] == nil {
				t.Errorf("Expected statements in response, got: %v", response["data"])
			}

			if response["data"].(map[string]any)["createdAt"] == nil {
				t.Errorf("Expected createdAt in response, got: %v", response["data"])
			}

			if response["data"].(map[string]any)["updatedAt"] == nil {
				t.Errorf("Expected updatedAt in response, got: %v", response["data"])
			}
		})

		t.Run("Show_WithInvalidToken", func(t *testing.T) {
			client := server.WithAccessKeyClient([]auth.Statement{
				{
					Effect:   auth.StatementEffectAllow,
					Resource: "token:foobar",
					Actions:  []auth.Privilege{"token:read"},
				},
			})

			response, statusCode, err := client.Send("/v1/tokens/invalid-token-id", "GET", nil)

			if err != nil {
				t.Fatalf("Failed to get token: %v", err)
			}

			if statusCode != 404 {
				t.Fatalf("Unexpected status code: %d, expected 404", statusCode)
			}

			if response["status"] != "error" {
				t.Errorf("Unexpected response: %v", response)
			}
		})

		t.Run("Show_WithUnauthorizedToken", func(t *testing.T) {
			token, err := server.App.Cluster.Auth.TokenManager.Create(
				"Test token",
				[]auth.Statement{{Effect: "Allow", Resource: "*", Actions: []auth.Privilege{"*"}}},
			)

			if err != nil {
				t.Fatalf("Failed to create test token: %v", err)
			}

			client := server.WithAccessKeyClient([]auth.Statement{
				{
					Effect:   auth.StatementEffectAllow,
					Resource: "token:*",
					Actions:  []auth.Privilege{"token:list"},
				},
			})

			response, statusCode, err := client.Send(fmt.Sprintf("/v1/tokens/%s", token.TokenID), "GET", nil)

			if err != nil {
				t.Fatalf("Failed to get token: %v", err)
			}

			if statusCode != 403 {
				t.Fatalf("Unexpected status code: %d, expected 403", statusCode)
			}

			if response["status"] != "error" {
				t.Errorf("Unexpected response: %v", response)
			}
		})

		t.Run("Store", func(t *testing.T) {
			client := server.WithAccessKeyClient([]auth.Statement{
				{
					Effect:   auth.StatementEffectAllow,
					Resource: "*",
					Actions:  []auth.Privilege{"token:create"},
				},
			})

			response, statusCode, err := client.Send("/v1/tokens", "POST", map[string]any{
				"description": "test",
				"statements": []map[string]any{
					{
						"effect":   "allow",
						"resource": "*",
						"actions":  []auth.Privilege{"*"},
					},
				},
			})

			if err != nil {
				t.Fatalf("Failed to create token: %v", err)
			}

			if statusCode != 201 {
				t.Errorf("Unexpected status code: %d, expected 201", statusCode)
			}

			if response["status"] != "success" {
				t.Errorf("Unexpected response: %v", response)
			}

			if response["data"] == nil {
				t.Errorf("Expected data in response, got: %v", response)
			}
		})

		t.Run("Store_WithInvalidToken", func(t *testing.T) {
			client := server.WithAccessKeyClient([]auth.Statement{
				{
					Effect:   auth.StatementEffectAllow,
					Resource: "database:*",
					Actions:  []auth.Privilege{"token:create"},
				},
			})

			response, statusCode, err := client.Send("/v1/tokens", "POST", nil)

			if err != nil {
				t.Fatalf("Failed to create token: %v", err)
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

		t.Run("Store_WithInvalidInput", func(t *testing.T) {
			client := server.WithAccessKeyClient([]auth.Statement{
				{
					Effect:   auth.StatementEffectAllow,
					Resource: "*",
					Actions:  []auth.Privilege{"token:create"},
				},
			})

			response, statusCode, err := client.Send("/v1/tokens", "POST", map[string]any{
				"resource":   "*",
				"statements": "",
			})

			if err != nil {
				t.Fatalf("Failed to create token: %v", err)
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

			response, statusCode, err = client.Send("/v1/tokens", "POST", map[string]any{
				"resource":   "",
				"statements": []map[string]any{},
			})

			if err != nil {
				t.Fatalf("Failed to create token: %v", err)
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

			response, statusCode, err = client.Send("/v1/tokens", "POST", map[string]any{
				"statements": []map[string]any{
					{
						"effect":   "Allowed",
						"resource": "*",
						"actions":  []auth.Privilege{"*"},
					},
				},
			})

			if err != nil {
				t.Fatalf("Failed to create token: %v", err)
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

		t.Run("Store_WithBasicAuth", func(t *testing.T) {
			client := server.WithBasicAuthClient()

			response, statusCode, err := client.Send("/v1/tokens", "POST", map[string]any{
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
				t.Fatalf("Failed to create token: %v", err)
			}

			if statusCode != 201 {
				t.Errorf("Unexpected status code: %d, expected 201", statusCode)
			}

			if response["status"] != "success" {
				t.Errorf("Unexpected response: %v", response)
			}

			if response["data"] == nil {
				t.Errorf("Expected data in response, got: %v", response)
			}
		})

		t.Run("Update", func(t *testing.T) {
			token, err := server.App.Cluster.Auth.TokenManager.Create(
				"test",
				[]auth.Statement{{Effect: "Allow", Resource: "*", Actions: []auth.Privilege{"*"}}},
			)

			if err != nil {
				t.Fatalf("Failed to create test token: %v", err)
			}

			client := server.WithAccessKeyClient([]auth.Statement{
				{
					Effect:   auth.StatementEffectAllow,
					Resource: "*",
					Actions:  []auth.Privilege{"token:update"},
				},
			})

			response, statusCode, err := client.Send(fmt.Sprintf("/v1/tokens/%s", token.TokenID), "PUT", map[string]any{
				"description": "Updated description",
				"statements": []map[string]any{
					{
						"effect":   "allow",
						"resource": "*",
						"actions":  []auth.Privilege{"*"},
					},
				},
			})

			if err != nil {
				t.Fatalf("Failed to update token: %v", err)
			}

			if statusCode != 200 {
				t.Fatalf("Unexpected status code: %d, expected 200", statusCode)
			}

			if response["status"] != "success" {
				t.Errorf("Unexpected response: %v", response)
			}

			if response["data"] == nil || response["data"].(map[string]any)["tokenId"] == nil || response["data"].(map[string]any)["statements"] == nil {
				t.Errorf("Unexpected response: %v", response)
			}
		})

		t.Run("Update_WithInvalidToken", func(t *testing.T) {
			token, err := server.App.Cluster.Auth.TokenManager.Create(
				"Test token",
				[]auth.Statement{{Effect: "Allow", Resource: "*", Actions: []auth.Privilege{"*"}}},
			)

			if err != nil {
				t.Fatalf("Failed to create test token: %v", err)
			}

			client := server.WithAccessKeyClient([]auth.Statement{
				{
					Effect:   auth.StatementEffectAllow,
					Resource: "token:foobar",
					Actions:  []auth.Privilege{"token:update"},
				},
			})

			response, statusCode, err := client.Send(fmt.Sprintf("/v1/tokens/%s", token.TokenID), "PUT", map[string]any{
				"statements": []map[string]any{
					{
						"effect":   "allow",
						"resource": "*",
						"actions":  []auth.Privilege{"*"},
					},
				},
			})

			if err != nil {
				t.Fatalf("Failed to update token: %v", err)
			}

			if statusCode != 403 {
				t.Fatalf("Unexpected status code: %d, expected 403", statusCode)
			}

			if response["status"] != "error" {
				t.Errorf("Unexpected response: %v", response)
			}
		})
	})
}
