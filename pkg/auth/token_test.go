package auth_test

import (
	"encoding/json"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/server"
)

func TestToken(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		t.Run("NewToken", func(t *testing.T) {
			token := auth.NewToken(
				app.Auth.TokenManager,
				"tokenId",
				"tokenHash",
				"Description",
				[]auth.AccessKeyStatement{},
			)

			if token == nil {
				t.Fatal("Expected token to be non-nil")
			}

			if token.TokenID != "tokenId" {
				t.Errorf("Expected tokenId to be 'tokenId', got %s", token.TokenID)
			}

			if token.TokenHash != "tokenHash" {
				t.Errorf("Expected tokenHash to be 'tokenHash', got %s", token.TokenHash)
			}

			if token.Description != "Description" {
				t.Errorf("Expected description to be 'Description', got %s", token.Description)
			}

			if token.CreatedAt.IsZero() {
				t.Error("Expected CreatedAt to be set, got zero value")
			}

			if token.UpdatedAt.IsZero() {
				t.Error("Expected UpdatedAt to be set, got zero value")
			}
		})

		t.Run("TokenResponse", func(t *testing.T) {
			token := auth.NewToken(
				app.Auth.TokenManager,
				"tokenId",
				"tokenHash",
				"Description",
				[]auth.AccessKeyStatement{},
			)

			jsonData, err := json.Marshal(token.ToResponse())

			if err != nil {
				t.Error(err)
			}

			if jsonData == nil {
				t.Error("Expected JSON data to be non-empty")
			}

			var result map[string]any

			if err := json.Unmarshal(jsonData, &result); err != nil {
				t.Errorf("Failed to unmarshal JSON: %v", err)
			}

			if result["token_id"] != "tokenId" {
				t.Errorf("Expected tokenId to be 'tokenId', got %v", result["token_id"])
			}
		})

		t.Run("DeleteToken", func(t *testing.T) {
			token, err := app.Auth.TokenManager.Create(
				"Test description",
				[]auth.AccessKeyStatement{},
			)

			if err != nil {
				t.Error(err)
			}

			tokenId := token.TokenID

			if err := token.Delete(); err != nil {
				t.Error(err)
			}

			token, err = app.Auth.TokenManager.Get(tokenId)

			if err == nil {
				t.Error("Expected token to be nil")
			}

			if token != nil {
				t.Errorf("Expected token to be nil, got %v", token)
			}
		})

		t.Run("UpdateToken", func(t *testing.T) {
			token, err := app.Auth.TokenManager.Create(
				"Description",
				[]auth.AccessKeyStatement{},
			)

			if err != nil {
				t.Error(err)
			}

			tokenId := token.TokenID
			statements := []auth.AccessKeyStatement{
				{
					Resource: "*",
					Actions:  []auth.Privilege{"*"},
				},
			}

			updatedAt := token.UpdatedAt

			if err := token.Update("Updated Description", statements); err != nil {
				t.Error(err)
			}

			token, err = app.Auth.TokenManager.Get(tokenId)

			if err != nil {
				t.Error(err)
			}

			if token == nil {
				t.Fatal("Expected token to be non-nil")
			}

			if token.Description != "Updated Description" {
				t.Errorf("Expected description to be 'Updated Description', got %s", token.Description)
			}

			if len(token.Statements) != 1 {
				t.Errorf("Expected statements to have length 1, got %d", len(token.Statements))
			}

			if token.Statements[0].Resource != "*" {
				t.Errorf("Expected resource to be '*', got %s", token.Statements[0].Resource)
			}

			if len(token.Statements[0].Actions) != 1 {
				t.Errorf("Expected actions to have length 1, got %d", len(token.Statements[0].Actions))
			}

			if token.Statements[0].Actions[0] != "*" {
				t.Errorf("Expected action to be '*', got %s", token.Statements[0].Actions[0])
			}

			if token.UpdatedAt.Equal(updatedAt) {
				t.Error("Expected UpdatedAt to be updated, but it is still the same as before")
			}
		})
	})
}
