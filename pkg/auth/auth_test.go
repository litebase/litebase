package auth_test

import (
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/server"
)

func TestAuth(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		// Access Key
		accessKey, err := app.Auth.AccessKeyManager.Create(
			"Description",
			[]auth.Statement{},
		)

		if err != nil {
			t.Fatalf("failed to create access key: %v", err)
		}

		retrievedAccessKey, err := app.Auth.GetCredential(accessKey.AccessKeyID, "Litebase-HMAC-SHA256")

		if err != nil {
			t.Fatalf("failed to get access key: %v", err)
		}

		if retrievedAccessKey.Type() != auth.CredentialTypeAccessKey {
			t.Fatalf("expected access key credential type, got %v", retrievedAccessKey.Type())
		}

		// Token
		token, err := app.Auth.TokenManager.Create(
			"",
			[]auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			},
		)

		if err != nil {
			t.Fatalf("failed to create token: %v", err)
		}

		retrievedToken, err := app.Auth.GetCredential(token.TokenID, "Bearer")

		if err != nil {
			t.Fatalf("failed to get token: %v", err)
		}

		if retrievedToken.Type() != auth.CredentialTypeToken {
			t.Fatalf("expected token credential type, got %v", retrievedToken.Type())
		}

		// User
		user, err := app.Auth.UserManager.Create(
			"testuser",
			"testpassword123",
			"",
			[]auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			},
		)

		if err != nil {
			t.Fatalf("failed to create user: %v", err)
		}

		retrievedUser, err := app.Auth.GetCredential(user.Username, "Basic")

		if err != nil {
			t.Fatalf("failed to get user: %v", err)
		}

		if retrievedUser.Type() != auth.CredentialTypeBasicAuth {
			t.Fatalf("expected user credential type, got %v", retrievedUser.Type())
		}
	})
}
func TestAuthBroadcast(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		auth := server.App.Auth

		var key, value string

		auth.Broadcaster(func(k string, v string) {
			key = k
			value = v
		})

		auth.Broadcast("testKey", "testValue")

		if key != "testKey" || value != "testValue" {
			t.Fatalf("Expected broadcast to be 'testKey: testValue', got '%s: %s'", key, value)
		}
	})
}
