package auth_test

import (
	"fmt"
	"net/http"
	"net/url"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
	appHttp "github.com/litebase/litebase/pkg/http"
	"github.com/litebase/litebase/pkg/server"
)

func TestRequestCredential(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		t.Run("CaptureRequestCredential_WithAccessKey", func(t *testing.T) {
			db := test.MockDatabase(app)

			databaseUrl := fmt.Sprintf("localhost:8080/databases/%s/%s", db.DatabaseKey.DatabaseName, db.DatabaseKey.DatabaseBranchName)

			token := auth.SignRequest(
				db.AccessKey.AccessKeyID,
				db.AccessKey.AccessKeySecret,
				"GET",
				"/",
				map[string]string{
					"Content-Type": "application/json",
				},
				[]byte{},
				map[string]string{},
			)

			baseRequest := &http.Request{
				Host:   databaseUrl,
				Method: http.MethodGet,
				Header: map[string][]string{
					"Content-Type":  {"application/json"},
					"Authorization": {fmt.Sprintf("Litebase-HMAC-SHA256 %s", token)},
				},
				URL: &url.URL{
					Host: databaseUrl,
				},
			}

			request := appHttp.NewRequest(
				app.Cluster,
				app.DatabaseManager,
				app.LogManager,
				baseRequest,
			)

			credential := auth.CaptureRequestCredential(
				app.Auth,
				request.Headers().Get("Authorization"),
			)

			if credential.Type() != auth.RequestCredentialTypeAccessKey {
				t.Fatal("Expected AccessKey credential type")
			}

			if !credential.IsAccessKey() {
				t.Fatal("Expected AccessKey credential type")
			}

			if credential.AccessKey().AccessKeyID != db.AccessKey.AccessKeyID {
				t.Errorf("Expected AccessKeyID %s, got %s",
					db.AccessKey.AccessKeyID, credential.AccessKey().AccessKeyID)
			}

			if !credential.Valid() {
				t.Fatal("Expected valid AccessKey credential")
			}
		})

		t.Run("CaptureRequestCredential_WithBasicAuth", func(t *testing.T) {
			user, err := app.Auth.UserManager.Create(
				"testuser",
				"testpassword123",
				[]auth.AccessKeyStatement{
					{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
				},
			)

			if err != nil {
				t.Fatalf("failed to create user: %v", err)
			}

			baseRequest := &http.Request{
				Host:   "http://localhost/foo",
				Method: http.MethodGet,
				Header: map[string][]string{
					"Content-Type": {"application/json"},
				},
				URL: &url.URL{
					Host: "http://localhost/foo",
				},
			}

			baseRequest.SetBasicAuth(user.Username, "testpassword123")

			request := appHttp.NewRequest(
				app.Cluster,
				app.DatabaseManager,
				app.LogManager,
				baseRequest,
			)

			credential := auth.CaptureRequestCredential(
				app.Auth,
				request.Headers().Get("Authorization"),
			)

			if credential.Type() != auth.RequestCredentialTypeBasicAuth {
				t.Fatal("Expected BasicAuth credential type")
			}

			if !credential.IsBasicAuth() {
				t.Fatal("Expected BasicAuth credential type")
			}

			if credential.User().Username != user.Username {
				t.Errorf("Expected Username %s, got %s",
					user.Username, credential.User().Username)
			}

			if !credential.Valid() {
				t.Fatal("Expected valid BasicAuth credential")
			}
		})

		t.Run("CaptureRequestCredential_WithBasicToken", func(t *testing.T) {
			token, err := app.Auth.TokenManager.Create(
				"",
				[]auth.AccessKeyStatement{
					{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
				},
			)

			if err != nil {
				t.Fatalf("failed to create user: %v", err)
			}

			tokenValue, err := token.Value()

			if err != nil {
				t.Fatalf("failed to get token value: %v", err)
			}

			baseRequest := &http.Request{
				Host:   "http://localhost/foo",
				Method: http.MethodGet,
				Header: map[string][]string{
					"Content-Type":  {"application/json"},
					"Authorization": {fmt.Sprintf("Bearer %s", tokenValue)},
				},
				URL: &url.URL{
					Host: "http://localhost/foo",
				},
			}

			request := appHttp.NewRequest(
				app.Cluster,
				app.DatabaseManager,
				app.LogManager,
				baseRequest,
			)

			credential := auth.CaptureRequestCredential(
				app.Auth,
				request.Headers().Get("Authorization"),
			)

			if credential.Type() != auth.RequestCredentialTypeToken {
				t.Fatal("Expected Token credential type")
			}

			if !credential.IsToken() {
				t.Fatal("Expected Token credential type")
			}

			if credential.Token().TokenID != token.TokenID {
				t.Errorf("Expected Token ID %s, got %s",
					token.TokenID, credential.Token().TokenID)
			}

			if !credential.Valid() {
				t.Fatal("Expected valid Token credential")
			}
		})

		t.Run("Invalid", func(t *testing.T) {
			invalidCredential := auth.RequestCredential{}

			if !invalidCredential.Invalid() {
				t.Fatal("Expected invalid credential")
			}
		})
	})
}
