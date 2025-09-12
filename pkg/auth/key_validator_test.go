package auth_test

import (
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/config"
	"github.com/litebase/litebase/pkg/server"
)

func TestValidateEncryptionKey(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		objectFS := app.Cluster.ObjectFS()
		testKey := app.Config.EncryptionKey

		t.Run("ValidateEncryptionKey with empty key", func(t *testing.T) {
			err := auth.ValidateEncryptionKey("", objectFS)

			if err == nil {
				t.Error("expected error for empty encryption key")
			}

			if err.Error() != "encryption key cannot be empty" {
				t.Errorf("unexpected error message: %s", err.Error())
			}
		})

		t.Run("ValidateEncryptionKey with valid key", func(t *testing.T) {
			err := auth.ValidateEncryptionKey(testKey, objectFS)

			if err != nil {
				t.Errorf("expected no error for valid key, got: %v", err)
			}
		})

		t.Run("ValidateEncryptionKey with invalid key", func(t *testing.T) {
			invalidKey := "invalid_key_1234567890abcdef1234567890abcdef12345678"

			err := auth.ValidateEncryptionKey(invalidKey, objectFS)

			if err == nil {
				t.Error("expected error for invalid encryption key")
			}

			if err.Error() != "provided encryption key does not match the stored key checksum" {
				t.Errorf("unexpected error message: %s", err.Error())
			}
		})

		t.Run("ValidateEncryptionKeyWithConfig", func(t *testing.T) {
			err := auth.ValidateEncryptionKeyWithConfig(app.Config, objectFS)

			if err != nil {
				t.Errorf("expected no error for valid config, got: %v", err)
			}

			// Test with invalid key in config
			invalidConfig := &config.Config{EncryptionKey: "invalid_key_1234567890abcdef1234567890abcdef12345678"}

			err = auth.ValidateEncryptionKeyWithConfig(invalidConfig, objectFS)

			if err == nil {
				t.Error("expected error for invalid key in config")
			}
		})
	})

	// Test case for when no stored key exists - use a fresh app setup
	t.Run("ValidateEncryptionKey with no stored key", func(t *testing.T) {
		test.RunWithApp(t, func(app *server.App) {
			objectFS := app.Cluster.ObjectFS()
			testKey := "test_encryption_key_1234567890abcdef1234567890abcdef12345678"

			// Clear the .key file to simulate no stored key
			objectFS.Remove(".key")

			err := auth.ValidateEncryptionKey(testKey, objectFS)

			if err == nil {
				t.Error("expected error when no stored key exists")
			}

			if err.Error() != "no stored encryption key hash found in .key file" {
				t.Errorf("unexpected error message: %s", err.Error())
			}
		})
	})
}
