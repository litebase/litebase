package auth_test

import (
	"fmt"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/database"
	"github.com/litebase/litebase/pkg/server"
)

func TestTokenManager(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		t.Run("NewTokenManager", func(t *testing.T) {
			a := auth.NewAuth(
				app.Config,
				app.Cluster.NetworkFS(),
				app.Cluster.ObjectFS(),
				app.Cluster.TmpFS(),
				app.Cluster.TmpTieredFS(),
			)

			tm := auth.NewTokenManager(
				database.NewSystemDatabaseTokenStorage(
					a.Config,
					a.SecretsManager,
					app.DatabaseManager.SystemDatabase(),
				),
				a,
			)

			if tm == nil {
				t.Error("Expected NewTokenManager to return a non-nil TokenManager")
			}
		})

		t.Run("AllTokens", func(t *testing.T) {
			tm := app.Auth.TokenManager

			for i := range 10 {
				_, err := tm.Create(fmt.Sprintf("Description %d", i), []auth.Statement{{Effect: "Allow", Resource: "*", Actions: []auth.Privilege{"*"}}})

				if err != nil {
					t.Fatalf("Expected no error when creating token, got: %v", err)
				}
			}

			tokens, err := tm.All()

			if err != nil {
				t.Error("Expected All to return no error")
			}

			if len(tokens) != 10 {
				t.Errorf("Expected All to return 10 tokens, got %d", len(tokens))
			}
		})

		t.Run("AllTokenIDs", func(t *testing.T) {
			tm := app.Auth.TokenManager

			tokens, err := tm.AllTokenIDs()

			if err != nil {
				t.Error("Expected AllTokenIDs to return no error")
			}

			currentTokenCount := len(tokens)

			for i := range 10 {
				_, err := tm.Create(fmt.Sprintf("Description %d", i), []auth.Statement{{Effect: "Allow", Resource: "*", Actions: []auth.Privilege{"*"}}})

				if err != nil {
					t.Fatalf("Expected no error when creating token, got: %v", err)
				}
			}

			expectedTokenCount := currentTokenCount + 10

			tokens, err = tm.AllTokenIDs()

			if err != nil {
				t.Error("Expected AllTokenIDs to return no error")
			}

			if len(tokens) != expectedTokenCount {
				t.Errorf("Expected AllTokenIDs to return %d tokens, got %d", expectedTokenCount, len(tokens))
			}
		})

		t.Run("Create", func(t *testing.T) {
			token, err := app.Auth.TokenManager.Create("Test token", []auth.Statement{{Effect: "Allow", Resource: "*", Actions: []auth.Privilege{"*"}}})

			if err != nil {
				t.Error("Expected Create to return no error")
			}

			if token.TokenID == "" {
				t.Error("Expected TokenID to not be an empty string")
			}
		})

		t.Run("GenerateTokenSecret", func(t *testing.T) {
			secret := app.Auth.TokenManager.GenerateTokenSecret()

			if secret == "" {
				t.Error("Expected GenerateTokenSecret to not return an empty string")
			}
		})

		t.Run("GenerateTokenID", func(t *testing.T) {
			tokenID, err := app.Auth.TokenManager.GenerateTokenID()

			if err != nil {
				t.Error("Expected GenerateTokenID to return no error")
			}

			if tokenID == "" {
				t.Error("Expected GenerateTokenID to not return an empty string")
			}
		})

		t.Run("Get", func(t *testing.T) {
			token, err := app.Auth.TokenManager.Create("Test token", []auth.Statement{{Effect: "Allow", Resource: "*", Actions: []auth.Privilege{"*"}}})

			if err != nil {
				t.Error("Expected Create to return no error")
			}

			token2, err := app.Auth.TokenManager.Get(token.TokenID)

			if err != nil {
				t.Error("Expected Get to return no error")
			}

			if token2.TokenID != token.TokenID {
				t.Error("Expected TokenID to match")
			}
		})

		t.Run("Purge", func(t *testing.T) {
			token, err := app.Auth.TokenManager.Create("Test token", []auth.Statement{{Effect: "Allow", Resource: "*", Actions: []auth.Privilege{"*"}}})

			if err != nil {
				t.Error("Expected Create to return no error")
			}

			token1, err := app.Auth.TokenManager.Get(token.TokenID)

			if err != nil {
				t.Errorf("Expected Get to return no error before deletion, got %v", err)
			}

			if token1 == nil {
				t.Fatal("Expected Get to return a non-nil Token")
			}

			err = token.Delete()

			if err != nil {
				t.Errorf("Expected Delete to return no error, got %v", err)
			}

			err = app.Auth.TokenManager.Purge(token.TokenID)

			if err != nil {
				t.Error("Expected Purge to return no error")
			}

			token1, err = app.Auth.TokenManager.Get(token.TokenID)

			if err == nil {
				t.Error("Expected Get to return an error after Purge")
			}

			if token1 != nil {
				t.Error("Expected Get to return a nil Token after Purge")
			}
		})

		t.Run("PurgeAll", func(t *testing.T) {
			for i := range 10 {
				_, err := app.Auth.TokenManager.Create(
					fmt.Sprintf("Test token %d", i),
					[]auth.Statement{{Effect: "Allow", Resource: "*", Actions: []auth.Privilege{"*"}}},
				)

				if err != nil {
					t.Fatalf("Expected no error when creating token, got: %v", err)
				}
			}

			err := app.Auth.TokenManager.PurgeAll()

			if err != nil {
				t.Error("Expected PurgeAll to return no error")
			}
		})
	})
}
