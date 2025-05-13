package auth_test

import (
	"testing"

	"github.com/litebase/litebase/common/config"
	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/server"
	"github.com/litebase/litebase/server/auth"
)

func TestNewSecretsManager(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		a := auth.NewAuth(&config.Config{}, app.Cluster.ObjectFS(), app.Cluster.TmpFS())
		sm := auth.NewSecretsManager(a, a.Config, a.ObjectFS, a.TmpFS)

		if sm == nil {
			t.Error("Expected NewSecretsManager to return a non-nil SecretsManager")
		}
	})
}

func TestSecretsManagerDecrypt(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		str := "test"

		encrypted, err := app.Auth.SecretsManager.Encrypt(app.Config.Signature, []byte(str))

		if err != nil {
			t.Error("Expected Encrypt to return a non-nil error")
		}

		decrypted, err := app.Auth.SecretsManager.Decrypt(app.Config.Signature, encrypted)

		if err != nil {
			t.Error("Expected Decrypt to return a non-nil error")
		}

		if decrypted.Value != str {
			t.Error("Expected Decrypt to return the same string as Encrypt")
		}
	})
}

func TestSecretsManagerDecryptFor(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		accessKey, err := app.Auth.AccessKeyManager.Create()

		if err != nil {
			t.Error("Expected Create to return a non-nil error")
		}

		str := "test"

		encrypted, err := app.Auth.SecretsManager.EncryptFor(accessKey.AccessKeyId, str)

		if err != nil {
			t.Error("Expected Encrypt to return a non-nil error")
		}

		decrypted, err := app.Auth.SecretsManager.DecryptFor(accessKey.AccessKeyId, accessKey.AccessKeySecret, encrypted)

		if err != nil {
			t.Error("Expected DecryptFor to return a non-nil error")
		}

		if decrypted.Value != str {
			t.Error("Expected DecryptFor to return the same string as Encrypt")
		}
	})
}

func TestSecretsManagerDeleteDatabaseAccessKey(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		err := app.Auth.SecretsManager.StoreDatabaseKey("databaseKey", "databaseId", "branchId")

		if err != nil {
			t.Error("Expected StoreDatabaseKey to return a non-nil error")
		}

		err = app.Auth.SecretsManager.DeleteDatabaseKey("databaseKey")

		if err != nil {
			t.Error("Expected DeleteDatabaseAccessKey to return a non-nil error")
		}
	})
}

func TestSecretsManagerEncrypt(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		str := "test"

		encrypted, err := app.Auth.SecretsManager.Encrypt(app.Config.Signature, []byte(str))

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
}

func TestSecretsManagerEncryptFor(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		accessKey, err := app.Auth.AccessKeyManager.Create()

		if err != nil {
			t.Error("Expected Create to return a non-nil error")
		}

		str := "test"

		encrypted, err := app.Auth.SecretsManager.EncryptFor(accessKey.AccessKeyId, str)

		if err != nil {
			t.Error("Expected Encrypt to return a non-nil error")
		}

		if encrypted == "" {
			t.Error("Expected Encrypt to not return an empty string")
		}

		if encrypted == str {
			t.Error("Expected Encrypt to return a different string")
		}
	})
}

func TestSecretsManagerEncrypter(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		encrypter := app.Auth.SecretsManager.Encrypter(app.Config.Signature)

		if encrypter == nil {
			t.Error("Expected Encrypter to return a non-nil Encrypter")
		}
	})
}

func TestSecretsManagerFlushTransients(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		err := app.Auth.SecretsManager.StoreDatabaseKey("databaseKey", "databaseId", "branchId")

		if err != nil {
			t.Error("Expected StoreDatabaseKey to return a non-nil error")
		}

		err = app.Auth.SecretsManager.FlushTransients()

		if err != nil {
			t.Error("Expected FlushTransients to return a non-nil error")
		}
	})
}

func TestSecretsManagerGetAccessKeySecret(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		accessKey, err := app.Auth.AccessKeyManager.Create()

		if err != nil {
			t.Error("Expected Create to return a non-nil error")
		}

		secret, err := app.Auth.SecretsManager.GetAccessKeySecret(accessKey.AccessKeyId)

		if err != nil {
			t.Error("Expected GetAccessKeySecret to return a non-nil error")
		}

		if secret == "" {
			t.Error("Expected GetAccessKeySecret to not return an empty string")
		}

		if secret != accessKey.AccessKeySecret {
			t.Error("Expected GetAccessKeySecret to return the same secret as the access key")
		}
	})
}

func TestSecretsManagerInit(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		err := app.Auth.SecretsManager.Init()

		if err != nil {
			t.Error("Expected Init to return a non-nil error")
		}
	})
}

func TestSecretsManagerPurgeDatabaseSettings(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		err := app.Auth.SecretsManager.StoreDatabaseKey("databaseKey", "databaseId", "branchId")

		if err != nil {
			t.Error("Expected StoreDatabaseKey to return a non-nil error")
		}

		err = app.Auth.SecretsManager.PurgeDatabaseSettings("databaseId", "branchId")

		if err != nil {
			t.Error("Expected PurgeDatabaseSettings to return a non-nil error")
		}
	})
}

func TestSecretsManagerPurgeExpiredSecrets(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		err := app.Auth.SecretsManager.PurgeExpiredSecrets()

		if err != nil {
			t.Error("Expected PurgeExpiredSecrets to return a non-nil error")
		}
	})
}

func TestSecretsManagerSecretsPath(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		path := app.Auth.SecretsManager.SecretsPath("signature", "path")

		if path == "" {
			t.Error("Expected SecretsPath to not return an empty string")
		}
	})
}

func TestSecretsManagerStoreAccessKey(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		accessKey, err := app.Auth.AccessKeyManager.Create()

		if err != nil {
			t.Error("Expected Create to return a non-nil error")
		}

		err = app.Auth.SecretsManager.StoreAccessKey(accessKey)

		if err != nil {
			t.Error("Expected StoreAccessKey to return a non-nil error")
		}

		accessKeyCheck, err := app.Auth.AccessKeyManager.Get(accessKey.AccessKeyId)

		if err != nil {
			t.Error("Expected GetAccessKeySecret to return a non-nil error")
		}

		if accessKeyCheck == nil {
			t.Fatal("Expected GetAccessKeySecret to return the same secret as the access key")
		}

		if accessKeyCheck.AccessKeyId != accessKey.AccessKeyId {
			t.Error("Expected AccessKeyId to match")
		}
	})
}

func TestSecretsManagerStoreDatabaseKey(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		err := app.Auth.SecretsManager.StoreDatabaseKey("databaseKey", "databaseId", "branchId")

		if err != nil {
			t.Error("Expected StoreDatabaseKey to return a non-nil error")
		}

		databaseKey, err := app.Auth.SecretsManager.GetDatabaseKey("databaseKey")

		if err != nil {
			t.Error("Expected GetDatabaseKey to return a non-nil error")
		}

		if databaseKey == nil {
			t.Fatal("Expected GetDatabaseKey to return a non-nil database key")
		}

		if databaseKey.DatabaseId != "databaseId" {
			t.Error("Expected DatabaseId to match")
		}
	})
}
