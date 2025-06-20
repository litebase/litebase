package auth_test

import (
	"bytes"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/server"
)

func TestKeyEncrypter_Decrypt(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		keyEncrypter := auth.NewKeyEncrypter(app.Auth.SecretsManager, app.Config.Signature)
		testData := []byte("test data for decryption")

		// First encrypt some data
		encrypted, err := keyEncrypter.Encrypt(testData)

		if err != nil {
			t.Fatalf("Failed to encrypt test data: %v", err)
		}

		// Then decrypt it
		decrypted, err := keyEncrypter.Decrypt(encrypted)

		if err != nil {
			t.Errorf("Expected Decrypt to succeed, got error: %v", err)
		}

		if decrypted.Value != string(testData) {
			t.Errorf("Expected decrypted value to be %q, got %q", string(testData), decrypted.Value)
		}

		if decrypted.Key == "" {
			t.Error("Expected decrypted result to contain a non-empty Key")
		}
	})
}

func TestKeyEncrypter_DecryptInvalidData(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		keyEncrypter := auth.NewKeyEncrypter(app.Auth.SecretsManager, app.Config.Signature)

		// Test with invalid base64
		_, err := keyEncrypter.Decrypt([]byte("invalid base64 data!"))

		if err == nil {
			t.Error("Expected Decrypt to fail with invalid base64 data")
		}

		// Test with empty data
		_, err = keyEncrypter.Decrypt([]byte(""))

		if err == nil {
			t.Error("Expected Decrypt to fail with empty data")
		}

		// Test with valid base64 but invalid JSON
		invalidJSON := []byte("dGVzdA==") // base64 for "test"

		_, err = keyEncrypter.Decrypt(invalidJSON)
		if err == nil {
			t.Error("Expected Decrypt to fail with invalid JSON structure")
		}
	})
}

func TestKeyEncrypter_DifferentSignatures(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		// Create key encrypters with different signatures
		keyEncrypter1 := auth.NewKeyEncrypter(app.Auth.SecretsManager, app.Config.Signature)

		// Use SignatureNext if available, otherwise create a test signature
		var signature2 string

		if app.Config.SignatureNext != "" {
			signature2 = app.Config.SignatureNext
		} else {
			signature2 = test.CreateHash(64)
			app.Config.SignatureNext = signature2
		}

		keyEncrypter2 := auth.NewKeyEncrypter(app.Auth.SecretsManager, signature2)

		testData := []byte("test data for different signatures")

		// Encrypt with first encrypter
		encrypted1, err := keyEncrypter1.Encrypt(testData)
		if err != nil {
			t.Fatalf("Failed to encrypt with first encrypter: %v", err)
		}

		// Try to decrypt with second encrypter (should fail)
		_, err = keyEncrypter2.Decrypt(encrypted1)

		if err == nil {
			t.Error("Expected decryption to fail when using different signature")
		}
	})
}

func TestKeyEncrypter_Encrypt(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		keyEncrypter := auth.NewKeyEncrypter(app.Auth.SecretsManager, app.Config.Signature)
		testData := []byte("test data for encryption")

		encrypted, err := keyEncrypter.Encrypt(testData)

		if err != nil {
			t.Errorf("Expected Encrypt to succeed, got error: %v", err)
		}

		if encrypted == nil {
			t.Error("Expected Encrypt to return non-nil encrypted data")
		}

		if len(encrypted) == 0 {
			t.Error("Expected Encrypt to return non-empty encrypted data")
		}

		// Ensure encrypted data is different from original
		if bytes.Equal(encrypted, testData) {
			t.Error("Expected encrypted data to be different from original data")
		}
	})
}

func TestKeyEncrypter_EncryptDecryptRoundTrip(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		keyEncrypter := auth.NewKeyEncrypter(app.Auth.SecretsManager, app.Config.Signature)

		testCases := [][]byte{
			[]byte("simple test"),
			[]byte(""),
			[]byte("special characters: !@#$%^&*()"),
			[]byte(`{"json": "data", "number": 123}`),
			[]byte("long string: " + string(make([]byte, 1000))), // 1KB of zeros
		}

		for i, testData := range testCases {
			// Encrypt the data
			encrypted, err := keyEncrypter.Encrypt(testData)
			if err != nil {
				t.Errorf("Test case %d: Failed to encrypt: %v", i, err)
				continue
			}

			// Decrypt the data
			decrypted, err := keyEncrypter.Decrypt(encrypted)
			if err != nil {
				t.Errorf("Test case %d: Failed to decrypt: %v", i, err)
				continue
			}

			// Verify the round trip worked
			if decrypted.Value != string(testData) {
				t.Errorf("Test case %d: Round trip failed. Expected %q, got %q", i, string(testData), decrypted.Value)
			}
		}
	})
}

func TestKeyEncrypter_MultipleEncryptions(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		keyEncrypter := auth.NewKeyEncrypter(app.Auth.SecretsManager, app.Config.Signature)
		testData := []byte("test data")

		// Encrypt the same data multiple times
		encrypted1, err := keyEncrypter.Encrypt(testData)
		if err != nil {
			t.Fatalf("Failed first encryption: %v", err)
		}

		encrypted2, err := keyEncrypter.Encrypt(testData)
		if err != nil {
			t.Fatalf("Failed second encryption: %v", err)
		}

		// The encrypted results should be different (due to random IV)
		if bytes.Equal(encrypted1, encrypted2) {
			t.Error("Expected different encrypted outputs for same input (due to random IV)")
		}

		// Both should decrypt to the same value
		decrypted1, err := keyEncrypter.Decrypt(encrypted1)
		if err != nil {
			t.Errorf("Failed to decrypt first encryption: %v", err)
		}

		decrypted2, err := keyEncrypter.Decrypt(encrypted2)
		if err != nil {
			t.Errorf("Failed to decrypt second encryption: %v", err)
		}

		if decrypted1.Value != string(testData) {
			t.Errorf("First decryption failed: expected %q, got %q", string(testData), decrypted1.Value)
		}

		if decrypted2.Value != string(testData) {
			t.Errorf("Second decryption failed: expected %q, got %q", string(testData), decrypted2.Value)
		}
	})
}

func TestNewKeyEncrypter(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		keyEncrypter := auth.NewKeyEncrypter(app.Auth.SecretsManager, app.Config.Signature)

		if keyEncrypter == nil {
			t.Error("Expected NewKeyEncrypter to return a non-nil KeyEncrypter")
		}
	})
}
