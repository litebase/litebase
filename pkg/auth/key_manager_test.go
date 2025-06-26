package auth_test

import (
	"fmt"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/config"
	"github.com/litebase/litebase/pkg/server"
)

func TestKeyManager(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		t.Run("TestGetPrivateKey", func(t *testing.T) {
			privateKey, err := auth.GetPrivateKey(
				app.Config.EncryptionKey,
				app.Cluster.ObjectFS(),
			)

			if err != nil {
				t.Fatalf("Failed to get private key: %s", err.Error())
			}

			if privateKey == nil {
				t.Fatalf("Private key is nil")
			}
		})

		t.Run("TestHasKey", func(t *testing.T) {
			hasKey := auth.HasKey(
				app.Config.EncryptionKey,
				app.Cluster.ObjectFS(),
			)

			if !hasKey {
				t.Fatalf("Expected key to exist, but it does not")
			}
		})

		t.Run("TestNextEncryptionKey", func(t *testing.T) {
			test.RunWithApp(t, func(app *server.App) {
				err := auth.KeyManagerInit(
					app.Config,
					app.Auth.SecretsManager,
				)

				if err != nil {
					t.Fatalf("Failed to initialize key manager: %s", err.Error())
				}

				err = auth.NextEncryptionKey(
					app.Auth,
					app.Config,
					"test",
				)

				if err != nil {
					t.Fatalf("Failed to get public key: %s", err.Error())
				}
			})
		})
	})
}

func TestGetPrivateKeyWithObjectStorage(t *testing.T) {
	test.RunWithObjectStorage(t, func(app *server.App) {
		privateKey, err := auth.GetPrivateKey(
			app.Config.EncryptionKey,
			app.Cluster.ObjectFS(),
		)

		if err != nil {
			t.Fatalf("Failed to get private key: %s", err.Error())
		}

		if privateKey == nil {
			t.Fatalf("Private key is nil")
		}
	})
}

func TestKeyManagerInit(t *testing.T) {
	test.Run(t, func() {
		server := test.NewUnstartedTestServer(t)

		_, err := auth.GetPrivateKey(
			"test",
			server.App.Cluster.ObjectFS(),
		)

		if err == nil {
			t.Fatalf("Expected error when getting private key, but got nil")
		}

		err = auth.KeyManagerInit(
			server.App.Config,
			server.App.Auth.SecretsManager,
		)

		if err != nil {
			t.Fatalf("Failed to initialize key manager: %s", err.Error())
		}

		privateKey, err := auth.GetPrivateKey(
			server.App.Config.EncryptionKey,
			server.App.Cluster.ObjectFS(),
		)

		if err != nil {
			t.Fatalf("Failed to get private key: %s", err.Error())
		}

		if privateKey == nil {
			t.Fatalf("Private key is nil")
		}
	})
}

func TestKeyPath(t *testing.T) {
	test.Run(t, func() {
		server := test.NewUnstartedTestServer(t)

		err := auth.KeyManagerInit(
			server.App.Config,
			server.App.Auth.SecretsManager,
		)

		if err != nil {
			t.Fatalf("Failed to initialize key manager: %s", err.Error())
		}

		privateKeyPath := auth.KeyPath(
			"private",
			server.App.Config.EncryptionKey,
		)

		if privateKeyPath == "" {
			t.Fatalf("Private key path is empty")
		}

		if privateKeyPath != fmt.Sprintf("%s/private.key", config.EncryptionKeyHash(server.App.Config.EncryptionKey)) {
			t.Fatalf("Private key path is not correct: %s", privateKeyPath)
		}
	})
}
