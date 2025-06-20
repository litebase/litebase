package auth_test

import (
	"fmt"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/config"
	"github.com/litebase/litebase/pkg/server"
)

func TestGetPrivateKey(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		privateKey, err := auth.GetPrivateKey(
			app.Config.Signature,
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

func TestGetPrivateKeyWithObjectStorage(t *testing.T) {
	test.RunWithObjectStorage(t, func(app *server.App) {
		privateKey, err := auth.GetPrivateKey(
			app.Config.Signature,
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
			server.App.Config.Signature,
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
			server.App.Config.Signature,
		)

		if privateKeyPath == "" {
			t.Fatalf("Private key path is empty")
		}

		if privateKeyPath != fmt.Sprintf("%s/private.key", config.SignatureHash(server.App.Config.Signature)) {
			t.Fatalf("Private key path is not correct: %s", privateKeyPath)
		}
	})
}

func TestNextSignature(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		err := auth.KeyManagerInit(
			app.Config,
			app.Auth.SecretsManager,
		)

		if err != nil {
			t.Fatalf("Failed to initialize key manager: %s", err.Error())
		}

		err = auth.NextSignature(
			app.Auth,
			app.Config,
			app.Auth.SecretsManager,
			"test",
		)

		if err != nil {
			t.Fatalf("Failed to get public key: %s", err.Error())
		}
	})
}
