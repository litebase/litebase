package auth_test

import (
	"testing"

	"github.com/google/uuid"
	"github.com/litebase/litebase/common/config"
	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/server"
)

func TestNewSecretsManager(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
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
}

func TestSecretsManager_Decrypt(t *testing.T) {
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

func TestSecretsManager_DatabaseKeyStore(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		// Signature test
		databaseKeyStore, err := app.Auth.SecretsManager.DatabaseKeyStore(
			app.Config.Signature,
		)

		if databaseKeyStore == nil {
			t.Error("Expected DatabaseKeyStore to return a non-nil value")
		}

		if err != nil {
			t.Error("Expected DatabaseKeyStore to return a non-nil error")
		}

		// Signature next test
		app.Config.SignatureNext = test.CreateHash(64)

		databaseKeyStore, err = app.Auth.SecretsManager.DatabaseKeyStore(
			app.Config.SignatureNext,
		)

		if databaseKeyStore == nil {
			t.Error("Expected DatabaseKeyStore to return a non-nil value")
		}

		if err != nil {
			t.Errorf("Expected DatabaseKeyStore to return a non-nil error, got %v", err)
		}

		// Invalid database key store test
		databaseKeyStore, err = app.Auth.SecretsManager.DatabaseKeyStore(
			"foo",
		)

		if databaseKeyStore != nil {
			t.Error("Expected DatabaseKeyStore to return a nil value")
		}

		if err == nil {
			t.Error("Expected DatabaseKeyStore to return a nil error")
		}
	})
}

func TestSecretsManager_DeleteDatabaseAccessKey(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		err := app.Auth.SecretsManager.StoreDatabaseKey(
			"databaseKey",
			uuid.NewString(),
			uuid.NewString(),
		)

		if err != nil {
			t.Errorf("Expected StoreDatabaseKey to return a non-nil error, got %v", err)
		}

		err = app.Auth.SecretsManager.DeleteDatabaseKey("databaseKey")

		if err != nil {
			t.Error("Expected DeleteDatabaseAccessKey to return a non-nil error")
		}
	})
}

func TestSecretsManager_DeleteDatabaseKey(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		app.Config.SignatureNext = test.CreateHash(64)

		err := app.Auth.SecretsManager.StoreDatabaseKey(
			"databaseKey",
			uuid.NewString(),
			uuid.NewString(),
		)

		if err != nil {
			t.Errorf("Expected StoreDatabaseKey to return a non-nil error, got %v", err)
		}

		err = app.Auth.SecretsManager.DeleteDatabaseKey("databaseKey")

		if err != nil {
			t.Error("Expected DeleteDatabaseKey to return a non-nil error")
		}

		// Check that the key is deleted
		databaseKey, err := app.Auth.SecretsManager.GetDatabaseKey("databaseKey")

		if err == nil || databaseKey != nil {
			t.Error("Expected GetDatabaseKey to return an error or nil after deletion")
		}

		// Delete unknown database key
		err = app.Auth.SecretsManager.DeleteDatabaseKey("unknownKey")

		if err == nil {
			t.Error("Expected DeleteDatabaseKey to return a non-nil error")
		}
	})
}

func TestSecretsManager_DeleteDatabaseKey_WithSignatureNext(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		err := app.Auth.SecretsManager.StoreDatabaseKey(
			"databaseKey",
			uuid.NewString(),
			uuid.NewString(),
		)

		if err != nil {
			t.Errorf("Expected StoreDatabaseKey to return a non-nil error, got %v", err)
		}

		app.Config.SignatureNext = test.CreateHash(64)

		err = app.Auth.SecretsManager.DeleteDatabaseKey("databaseKey")

		if err == nil {
			t.Error("Expected DeleteDatabaseKey to return an error")
		}
	})
}

func TestSecretsManager_Encrypt(t *testing.T) {
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

func TestSecretsManager_Encrypter(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		encrypter := app.Auth.SecretsManager.Encrypter(app.Config.Signature)

		if encrypter == nil {
			t.Error("Expected Encrypter to return a non-nil Encrypter")
		}
	})
}

func TestSecretsManager_FlushTransients(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		err := app.Auth.SecretsManager.StoreDatabaseKey(
			"databaseKey",
			uuid.NewString(),
			uuid.NewString(),
		)

		if err != nil {
			t.Error("Expected StoreDatabaseKey to return a non-nil error")
		}

		err = app.Auth.SecretsManager.FlushTransients()

		if err != nil {
			t.Error("Expected FlushTransients to return a non-nil error")
		}
	})
}

func TestSecretsManager_GetAccessKeySecret(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		accessKey, err := app.Auth.AccessKeyManager.Create([]auth.AccessKeyStatement{{Effect: "Allow", Resource: "*", Actions: []auth.Privilege{"*"}}})

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

		secret, err = app.Auth.SecretsManager.GetAccessKeySecret(accessKey.AccessKeyId)

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
}

func TestSecretsManager_Init(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		err := app.Auth.SecretsManager.Init()

		if err != nil {
			t.Error("Expected Init to return a non-nil error")
		}
	})
}

func TestSecretsManager_PurgeDatabaseSettings(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		err := app.Auth.SecretsManager.StoreDatabaseKey(
			"databaseKey",
			uuid.NewString(),
			uuid.NewString(),
		)

		if err != nil {
			t.Error("Expected StoreDatabaseKey to return a non-nil error")
		}

		err = app.Auth.SecretsManager.PurgeDatabaseSettings("databaseId", "branchId")

		if err != nil {
			t.Error("Expected PurgeDatabaseSettings to return a non-nil error")
		}
	})
}

func TestSecretsManager_PurgeExpiredSecrets(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		err := app.Auth.SecretsManager.PurgeExpiredSecrets()

		if err != nil {
			t.Error("Expected PurgeExpiredSecrets to return a non-nil error")
		}
	})
}

func TestSecretsManager_SecretsPath(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		path := app.Auth.SecretsManager.SecretsPath("signature", "path")

		if path == "" {
			t.Error("Expected SecretsPath to not return an empty string")
		}
	})
}

func TestSecretsManager_StoreAccessKey(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		accessKey, err := app.Auth.AccessKeyManager.Create([]auth.AccessKeyStatement{{Effect: "Allow", Resource: "*", Actions: []auth.Privilege{"*"}}})

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

func TestSecretsManager_StoreDatabaseKey(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		databaseUUID := uuid.NewString()
		branchUUID := uuid.NewString()

		err := app.Auth.SecretsManager.StoreDatabaseKey(
			"databaseKey",
			databaseUUID,
			branchUUID,
		)

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

		if databaseKey.DatabaseId != databaseUUID {
			t.Error("Expected DatabaseId to match")
		}
	})
}
