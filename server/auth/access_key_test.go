package auth_test

import (
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/server"
	"github.com/litebase/litebase/server/auth"
)

func TestNewAccessKey(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		accessKey := auth.NewAccessKey(
			app.Auth.AccessKeyManager,
			"accessKeyId",
			"accessKeySecret",
			[]auth.AccessKeyStatement{},
		)

		if accessKey == nil {
			t.Fatal("Expected accessKey to be non-nil")
		}

		if accessKey.AccessKeyId != "accessKeyId" {
			t.Errorf("Expected accessKeyId to be 'accessKeyId', got %s", accessKey.AccessKeyId)
		}

		if accessKey.AccessKeySecret != "accessKeySecret" {
			t.Errorf("Expected accessKeySecret to be 'accessKeySecret', got %s", accessKey.AccessKeySecret)
		}
	})
}

func TestAccessKeyDelete(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		accessKey := auth.NewAccessKey(
			app.Auth.AccessKeyManager,
			"accessKeyId",
			"accessSecret",
			[]auth.AccessKeyStatement{},
		)

		err := app.Auth.SecretsManager.StoreAccessKey(accessKey)

		if err != nil {
			t.Error(err)
		}

		if err := accessKey.Delete(); err != nil {
			t.Error(err)
		}

		accessKey, err = app.Auth.AccessKeyManager.Get("accessKeyId")

		if err == nil {
			t.Error("Expected accessKey to be nil")
		}

		if accessKey != nil {
			t.Errorf("Expected accessKey to be nil, got %v", accessKey)
		}
	})
}

func TestAccessKeyUpdate(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		accessKey := auth.NewAccessKey(
			app.Auth.AccessKeyManager,
			"accessKeyId",
			"accessSecret",
			[]auth.AccessKeyStatement{},
		)

		err := app.Auth.SecretsManager.StoreAccessKey(accessKey)

		if err != nil {
			t.Error(err)
		}

		statements := []auth.AccessKeyStatement{
			{
				Resource: "*",
				Actions:  []auth.Privilege{"*"},
			},
		}

		if err := accessKey.Update(statements); err != nil {
			t.Error(err)
		}

		accessKey, err = app.Auth.AccessKeyManager.Get("accessKeyId")

		if err != nil {
			t.Error(err)
		}

		if accessKey == nil {
			t.Fatal("Expected accessKey to be non-nil")
		}

		if len(accessKey.Statements) != 1 {
			t.Errorf("Expected statements to have length 1, got %d", len(accessKey.Statements))
		}

		if accessKey.Statements[0].Resource != "*" {
			t.Errorf("Expected resource to be '*', got %s", accessKey.Statements[0].Resource)
		}

		if len(accessKey.Statements[0].Actions) != 1 {
			t.Errorf("Expected actions to have length 1, got %d", len(accessKey.Statements[0].Actions))
		}

		if accessKey.Statements[0].Actions[0] != "*" {
			t.Errorf("Expected action to be '*', got %s", accessKey.Statements[0].Actions[0])
		}
	})
}
