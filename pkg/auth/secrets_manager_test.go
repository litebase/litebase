package auth_test

import (
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/config"
	"github.com/litebase/litebase/pkg/server"
)

func TestSecretsManager(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		t.Run("NewSecretsManager", func(t *testing.T) {
			a := auth.NewAuth(
				&config.Config{},
				app.Cluster.NetworkFS(),
				app.Cluster.ObjectFS(),
				app.Cluster.TmpFS(),
				app.Cluster.TmpTieredFS(),
			)

			sm := auth.NewSecretsManager(
				a, a.Config,
				a.SecretsManager.NetworkFS,
				a.ObjectFS,
				a.TmpFS,
				app.Cluster.TmpTieredFS(),
			)

			if sm == nil {
				t.Error("Expected NewSecretsManager to return a non-nil SecretsManager")
			}
		})

		t.Run("Decrypt", func(t *testing.T) {
			str := "test"

			encrypted, err := app.Auth.SecretsManager.Encrypt(app.Config.EncryptionKey, []byte(str))

			if err != nil {
				t.Error("Expected Encrypt to return a non-nil error")
			}

			decrypted, err := app.Auth.SecretsManager.Decrypt(app.Config.EncryptionKey, encrypted)

			if err != nil {
				t.Error("Expected Decrypt to return a non-nil error")
			}

			if decrypted.Value != str {
				t.Error("Expected Decrypt to return the same string as Encrypt")
			}
		})

		t.Run("Encrypt", func(t *testing.T) {
			str := "test"

			encrypted, err := app.Auth.SecretsManager.Encrypt(app.Config.EncryptionKey, []byte(str))

			if err != nil {
				t.Error("Expected Encrypt to return a non-nil error")
			}

			if encrypted == nil {
				t.Error("Expected Encrypt to not return an empty string")
			}

			if string(encrypted) == str {
				t.Error("Expected Encrypt to return a different string")
			}
		})

		t.Run("Encrypter", func(t *testing.T) {
			encrypter := app.Auth.SecretsManager.Encrypter(app.Config.EncryptionKey)

			if encrypter == nil {
				t.Error("Expected Encrypter to return a non-nil Encrypter")
			}
		})

		t.Run("FlushTransients", func(t *testing.T) {
			accessKey, err := app.Auth.AccessKeyManager.Create("test", []auth.AccessKeyStatement{{Effect: "Allow", Resource: "*", Actions: []auth.Privilege{"*"}}})

			if err != nil {
				t.Fatal("Expected Create to return a non-nil error")
			}

			// Store a transient secret
			_, err = app.Auth.SecretsManager.GetAccessKeySecret(accessKey.AccessKeyID)

			if err != nil {
				t.Fatal("Expected GetAccessKeySecret to return a non-nil error")
			}

			err = app.Auth.SecretsManager.FlushTransients()

			if err != nil {
				t.Error("Expected FlushTransients to return a non-nil error")
			}
		})

		t.Run("GetAccessKeySecret", func(t *testing.T) {
			accessKey, err := app.Auth.AccessKeyManager.Create("test", []auth.AccessKeyStatement{{Effect: "Allow", Resource: "*", Actions: []auth.Privilege{"*"}}})

			if err != nil {
				t.Error("Expected Create to return a non-nil error")
			}

			secret, err := app.Auth.SecretsManager.GetAccessKeySecret(accessKey.AccessKeyID)

			if err != nil {
				t.Error("Expected GetAccessKeySecret to return a non-nil error")
			}

			if secret == "" {
				t.Error("Expected GetAccessKeySecret to not return an empty string")
			}

			if secret != accessKey.AccessKeySecret {
				t.Error("Expected GetAccessKeySecret to return the same secret as the access key")
			}

			secret, err = app.Auth.SecretsManager.GetAccessKeySecret(accessKey.AccessKeyID)

			if err != nil {
				t.Error("Expected GetAccessKeySecret to return a non-nil error")
			}

			if secret == "" {
				t.Error("Expected GetAccessKeySecret to not return an empty string")
			}

			if secret != accessKey.AccessKeySecret {
				t.Error("Expected GetAccessKeySecret to return the same secret as the access key")
			}

			// Non-existent access key test
			secret, err = app.Auth.SecretsManager.GetAccessKeySecret("unknownKey")

			if err == nil {
				t.Error("Expected GetAccessKeySecret to return a non-nil error")
			}

			if secret != "" {
				t.Error("Expected GetAccessKeySecret to return an empty string")
			}
		})

		t.Run("Init", func(t *testing.T) {
			err := app.Auth.SecretsManager.Init()

			if err != nil {
				t.Error("Expected Init to return a non-nil error")
			}
		})

		t.Run("PurgeDatabaseSettings", func(t *testing.T) {
			err := app.Auth.SecretsManager.PurgeDatabaseSettings("databaseId", "branchId")

			if err != nil {
				t.Error("Expected PurgeDatabaseSettings to return a non-nil error")
			}
		})

		t.Run("PurgeExpiredSecrets", func(t *testing.T) {
			err := app.Auth.SecretsManager.PurgeExpiredSecrets()

			if err != nil {
				t.Error("Expected PurgeExpiredSecrets to return a non-nil error")
			}
		})

		t.Run("SecretsPath", func(t *testing.T) {
			path := app.Auth.SecretsManager.SecretsPath("key", "path")

			if path == "" {
				t.Error("Expected SecretsPath to not return an empty string")
			}
		})

		t.Run("StoreAccessKey", func(t *testing.T) {
			accessKey, err := app.Auth.AccessKeyManager.Create("test", []auth.AccessKeyStatement{{Effect: "Allow", Resource: "*", Actions: []auth.Privilege{"*"}}})

			if err != nil {
				t.Error("Expected Create to return a non-nil error")
			}

			err = app.Auth.SecretsManager.StoreAccessKey(accessKey)

			if err != nil {
				t.Error("Expected StoreAccessKey to return a non-nil error")
			}

			accessKeyCheck, err := app.Auth.AccessKeyManager.Get(accessKey.AccessKeyID)

			if err != nil {
				t.Error("Expected GetAccessKeySecret to return a non-nil error")
			}

			if accessKeyCheck == nil {
				t.Fatal("Expected GetAccessKeySecret to return the same secret as the access key")
			}

			if accessKeyCheck.AccessKeyID != accessKey.AccessKeyID {
				t.Error("Expected AccessKeyID to match")
			}
		})
	})
}
