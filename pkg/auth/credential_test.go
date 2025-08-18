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

func TestCredential(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		t.Run("CaptureCredential_WithAccessKey", func(t *testing.T) {
			db := test.MockDatabase(app)

			databaseUrl := fmt.Sprintf("localhost:8080/databases/%s/%s", db.DatabaseKey.DatabaseName, db.DatabaseKey.DatabaseBranchName)

			token := auth.SignRequest(
				db.Credential.AccessKey().AccessKeyID,
				db.Credential.AccessKey().AccessKeySecret,
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

			credential := auth.CaptureCredential(
				app.Auth,
				request.Headers().Get("Authorization"),
			)

			if credential.Type() != auth.CredentialTypeAccessKey {
				t.Fatal("Expected AccessKey credential type")
			}

			if !credential.IsAccessKey() {
				t.Fatal("Expected AccessKey credential type")
			}

			if credential.AccessKey().AccessKeyID != db.Credential.AccessKey().AccessKeyID {
				t.Errorf("Expected AccessKeyID %s, got %s",
					db.Credential.AccessKey().AccessKeyID, credential.AccessKey().AccessKeyID)
			}

			if !credential.Valid() {
				t.Fatal("Expected valid AccessKey credential")
			}
		})

		t.Run("CaptureCredential_WithBasicAuth", func(t *testing.T) {
			user, err := app.Auth.UserManager.Create(
				"testuser",
				"testpassword123",
				[]auth.Statement{
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

			credential := auth.CaptureCredential(
				app.Auth,
				request.Headers().Get("Authorization"),
			)

			if credential.Type() != auth.CredentialTypeBasicAuth {
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

		t.Run("CaptureCredential_WithBasicToken", func(t *testing.T) {
			token, err := app.Auth.TokenManager.Create(
				"",
				[]auth.Statement{
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

			credential := auth.CaptureCredential(
				app.Auth,
				request.Headers().Get("Authorization"),
			)

			if credential.Type() != auth.CredentialTypeToken {
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

		t.Run("Hash", func(t *testing.T) {
			accessKey := auth.NewAccessKey(
				app.Auth.AccessKeyManager,
				"accessKeyId",
				"accessKeySecret",
				"Description",
				[]auth.Statement{},
			)

			credential := &auth.Credential{
				CredentialID:  accessKey.AccessKeyID,
				SignedHeaders: []string{"Authorization"},
			}

			hash := credential.Hash()

			if hash == [32]byte{} {
				t.Error("Expected hash to be non-zero")
			}
		})

		t.Run("Invalid", func(t *testing.T) {
			invalidCredential := auth.Credential{}

			if !invalidCredential.Invalid() {
				t.Fatal("Expected invalid credential")
			}
		})
	})
}
