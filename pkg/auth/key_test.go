package auth_test

import (
	"os"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/server"
)

func TestInitKey(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		objectFS := app.Cluster.ObjectFS()

		t.Run("InitKey with missing key", func(t *testing.T) {
			originalKey := app.Config.EncryptionKey
			app.Config.EncryptionKey = ""

			err := auth.InitKey(app.Config, objectFS)

			if err == nil {
				t.Error("expected error for missing encryption key")
			}

			if err.Error() != "the LITEBASE_ENCRYPTION_KEY environment variable is not set" {
				t.Errorf("unexpected error message: %s", err.Error())
			}

			app.Config.EncryptionKey = originalKey
		})

		t.Run("InitKey with valid key", func(t *testing.T) {
			err := auth.InitKey(app.Config, objectFS)

			if err != nil {
				t.Errorf("expected no error for valid key, got: %v", err)
			}
		})

		t.Run("InitKey with invalid key", func(t *testing.T) {
			originalKey := app.Config.EncryptionKey
			app.Config.EncryptionKey = "invalid_key_1234567890abcdef1234567890abcdef12345678"

			err := auth.InitKey(app.Config, objectFS)

			if err == nil {
				t.Error("expected error for invalid encryption key")
			}

			if err.Error() != "provided encryption key does not match the stored key checksum: provided encryption key does not match the stored key checksum" {
				t.Errorf("unexpected error message: %s", err.Error())
			}

			app.Config.EncryptionKey = originalKey
		})
	})
}

func TestStoreEncryptionKey(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		encryptionKey := test.CreateHash(64)

		err := auth.StoreEncryptionKey(app.Config, app.Cluster.ObjectFS(), encryptionKey)

		if err != nil {
			t.Fatalf("Failed to store encryption key: %v", err)
		}

		// check if the encryption key was stored on object storage
		if _, err := app.Cluster.ObjectFS().Stat(".key"); os.IsNotExist(err) {
			t.Fatalf("The encryption key file was not created")
		}

		// Ensure the encryption key is not stored in network storage
		if _, err := app.Cluster.NetworkFS().Stat(".encryption_key"); !os.IsNotExist(err) {
			t.Fatalf("The encryption key file should not exist in network storage")
		}

		// check if the encryption key was stored correctly
		file, err := app.Cluster.ObjectFS().Open(".key")

		if err != nil {
			t.Fatalf("Error opening the encryption key file: %s", err)
		}

		defer func() {
			err := file.Close()

			if err != nil {
				t.Fatalf("Error closing the encryption key file: %s", err)
			}
		}()

		encryptionKeyBytes := make([]byte, 64)

		if _, err := file.Read(encryptionKeyBytes); err != nil {
			t.Fatalf("Error reading the encryption key file: %s", err)
		}

		if string(encryptionKeyBytes) != auth.EncryptionKeyHash(encryptionKey) {
			t.Fatalf("The encryption key was not stored correctly")
		}
	})
}
