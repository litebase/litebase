package database_test

import (
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/database"
	"github.com/litebase/litebase/pkg/server"
)

func TestSystemDatabaseTokens(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		// Create a test token storage instance
		storage := database.NewSystemDatabaseTokenStorage(
			app.Config,
			app.Auth.SecretsManager,
			app.DatabaseManager.SystemDatabase(),
		)

		t.Run("Store and Get", func(t *testing.T) {
			token := &auth.Token{
				TokenID:     "test_token_id",
				TokenHash:   "test_token_hash",
				Description: "Test token",
				Statements: []auth.AccessKeyStatement{
					{
						Effect:   auth.AccessKeyEffectAllow,
						Resource: "*",
						Actions:  []auth.Privilege{"*"},
					},
				},
				CreatedAt: time.Now().UTC(),
				UpdatedAt: time.Now().UTC(),
			}

			err := storage.Store(token)

			if err != nil {
				t.Fatalf("Failed to store token: %v", err)
			}

			// Test Get
			retrievedToken, err := storage.Get("test_token_id")

			if err != nil {
				t.Fatalf("Failed to get token: %v", err)
			}

			if retrievedToken.TokenID != token.TokenID {
				t.Errorf("Expected TokenID %s, got %s", token.TokenID, retrievedToken.TokenID)
			}

			if retrievedToken.TokenHash != token.TokenHash {
				t.Errorf("Expected TokenHash %s, got %s", token.TokenHash, retrievedToken.TokenHash)
			}

			if retrievedToken.Description != token.Description {
				t.Errorf("Expected Description %s, got %s", token.Description, retrievedToken.Description)
			}

			if len(retrievedToken.Statements) != len(token.Statements) {
				t.Errorf("Expected %d statements, got %d", len(token.Statements), len(retrievedToken.Statements))
			} else {
				if retrievedToken.Statements[0].Effect != token.Statements[0].Effect {
					t.Errorf("Expected Effect %s, got %s", token.Statements[0].Effect, retrievedToken.Statements[0].Effect)
				}

				if retrievedToken.Statements[0].Resource != token.Statements[0].Resource {
					t.Errorf("Expected Resource %s, got %s", token.Statements[0].Resource, retrievedToken.Statements[0].Resource)
				}

				if len(retrievedToken.Statements[0].Actions) != len(token.Statements[0].Actions) {
					t.Errorf("Expected %d actions, got %d", len(token.Statements[0].Actions), len(retrievedToken.Statements[0].Actions))
				}
			}

			if retrievedToken.CreatedAt.Sub(token.CreatedAt).Abs() > time.Second {
				t.Errorf("CreatedAt timestamp differs too much: expected %v, got %v", token.CreatedAt, retrievedToken.CreatedAt)
			}

			if retrievedToken.UpdatedAt.Sub(token.UpdatedAt).Abs() > time.Second {
				t.Errorf("UpdatedAt timestamp differs too much: expected %v, got %v", token.UpdatedAt, retrievedToken.UpdatedAt)
			}
		})

		t.Run("Get non-existent token", func(t *testing.T) {
			_, err := storage.Get("non_existent_token")

			if err == nil {
				t.Error("Expected error when getting non-existent token, but got nil")
			}
		})

		t.Run("Store with complex statements", func(t *testing.T) {
			token := &auth.Token{
				TokenID:     "complex_token_id",
				TokenHash:   "complex_token_hash",
				Description: "Complex token with multiple statements",
				Statements: []auth.AccessKeyStatement{
					{
						Effect:   auth.AccessKeyEffectAllow,
						Resource: "database:*",
						Actions:  []auth.Privilege{auth.DatabasePrivilegeRead, auth.DatabasePrivilegeSelect},
					},
					{
						Effect:   auth.AccessKeyEffectDeny,
						Resource: "database:sensitive:*",
						Actions:  []auth.Privilege{auth.DatabasePrivilegeDelete},
					},
					{
						Effect:   auth.AccessKeyEffectAllow,
						Resource: "database:public:table:users",
						Actions:  []auth.Privilege{auth.DatabasePrivilegeInsert, auth.DatabasePrivilegeUpdate},
					},
				},
				CreatedAt: time.Now().UTC(),
				UpdatedAt: time.Now().UTC(),
			}

			err := storage.Store(token)

			if err != nil {
				t.Fatalf("Failed to store token: %v", err)
			}

			retrievedToken, err := storage.Get("complex_token_id")

			if err != nil {
				t.Fatalf("Failed to get token: %v", err)
			}

			if len(retrievedToken.Statements) != len(token.Statements) {
				t.Errorf("Expected %d statements, got %d", len(token.Statements), len(retrievedToken.Statements))
			}
		})
	})
}
