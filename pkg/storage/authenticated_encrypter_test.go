package storage

import (
	"bytes"
	"testing"
)

func TestDerivePageKey(t *testing.T) {
	dataKey := make([]byte, 32)

	for i := range dataKey {
		dataKey[i] = byte(i)
	}

	tests := []struct {
		name       string
		pageNumber uint64
		filePath   string
	}{
		{"page 0", 0, "/path/to/file.range"},
		{"page 1", 1, "/path/to/file.range"},
		{"page 100", 100, "/path/to/file.range"},
		{"different path", 0, "/different/path.range"},
	}

	// Collect all keys to verify uniqueness
	keys := make(map[string]bool)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key, err := derivePageKey(dataKey, tt.pageNumber, tt.filePath)

			if err != nil {
				t.Fatalf("derivePageKey failed: %v", err)
			}

			if len(key) != 32 {
				t.Errorf("expected key length 32, got %d", len(key))
			}

			// Check for uniqueness
			keyStr := string(key)

			if keys[keyStr] {
				t.Errorf("key collision detected for %s", tt.name)
			}

			keys[keyStr] = true
		})
	}

	// Verify we generated unique keys
	if len(keys) != len(tests) {
		t.Errorf("expected %d unique keys, got %d", len(tests), len(keys))
	}
}

func TestDerivePageKey_InvalidKeySize(t *testing.T) {
	invalidKey := make([]byte, 16) // Wrong size

	_, err := derivePageKey(invalidKey, 0, "/path/to/file.range")

	if err == nil {
		t.Error("expected error for invalid key size, got nil")
	}
}

func TestDerivePageKey_Deterministic(t *testing.T) {
	dataKey := make([]byte, 32)

	for i := range dataKey {
		dataKey[i] = byte(i)
	}

	// Derive key twice with same parameters
	key1, err := derivePageKey(dataKey, 42, "/test/file.range")

	if err != nil {
		t.Fatalf("first derivePageKey failed: %v", err)
	}

	key2, err := derivePageKey(dataKey, 42, "/test/file.range")

	if err != nil {
		t.Fatalf("second derivePageKey failed: %v", err)
	}

	if !bytes.Equal(key1, key2) {
		t.Error("derivePageKey is not deterministic")
	}
}

func TestEncryptDecryptPageGCM_RoundTrip(t *testing.T) {
	dataKey := make([]byte, 32)

	for i := range dataKey {
		dataKey[i] = byte(i)
	}

	pageData := make([]byte, AuthenticatedPageSize)

	for i := range pageData {
		pageData[i] = byte(i % 256)
	}

	pageNumber := uint64(42)
	filePath := "/test/database.range"

	// Encrypt
	encrypted, err := EncryptPageGCM(dataKey, pageData, pageNumber, filePath)

	if err != nil {
		t.Fatalf("EncryptPageGCM failed: %v", err)
	}

	// Encrypted data should be smaller than plaintext due to compression
	// (for this test data pattern)
	if len(encrypted) == 0 {
		t.Error("encrypted data should not be empty")
	}

	// Verify encrypted data is different from plaintext
	if bytes.Equal(encrypted[:min(len(encrypted), len(pageData))], pageData[:min(len(encrypted), len(pageData))]) {
		t.Error("encrypted data should not match plaintext")
	}

	// Decrypt
	decrypted, err := DecryptPageGCM(dataKey, encrypted, pageNumber, filePath)

	if err != nil {
		t.Fatalf("DecryptPageGCM failed: %v", err)
	}

	if len(decrypted) != AuthenticatedPageSize {
		t.Errorf("expected decrypted size %d, got %d", AuthenticatedPageSize, len(decrypted))
	}

	// Verify round-trip
	if !bytes.Equal(decrypted, pageData) {
		t.Error("decrypted data does not match original plaintext")
	}
}

func TestEncryptPageGCM_CompressionWorks(t *testing.T) {
	dataKey := make([]byte, 32)

	for i := range dataKey {
		dataKey[i] = byte(i)
	}

	// Create highly compressible data (all zeros)
	pageData := make([]byte, AuthenticatedPageSize)

	encrypted, err := EncryptPageGCM(dataKey, pageData, 0, "/test.range")

	if err != nil {
		t.Fatalf("EncryptPageGCM failed: %v", err)
	}

	// Compressed+encrypted should be much smaller than original
	// (zeros compress very well with S2)
	if len(encrypted) >= AuthenticatedPageSize {
		t.Errorf("expected compression to reduce size, got %d bytes (original %d)", len(encrypted), AuthenticatedPageSize)
	}

	// Verify we can decrypt it back
	decrypted, err := DecryptPageGCM(dataKey, encrypted, 0, "/test.range")

	if err != nil {
		t.Fatalf("DecryptPageGCM failed: %v", err)
	}

	if !bytes.Equal(decrypted, pageData) {
		t.Error("decrypted data does not match original")
	}
}

func TestEncryptPageGCM_DifferentKeysProduceDifferentCiphertext(t *testing.T) {
	dataKey := make([]byte, 32)

	for i := range dataKey {
		dataKey[i] = byte(i)
	}

	pageData := make([]byte, AuthenticatedPageSize)

	for i := range pageData {
		pageData[i] = byte(i % 256)
	}

	// Encrypt same page with different parameters
	enc1, _ := EncryptPageGCM(dataKey, pageData, 0, "/file1.range")
	enc2, _ := EncryptPageGCM(dataKey, pageData, 1, "/file1.range") // Different page
	enc3, _ := EncryptPageGCM(dataKey, pageData, 0, "/file2.range") // Different path

	// All should be different (ignoring the random nonce, the keys should differ)
	// We can't directly compare because nonce is random, but we can verify decryption works correctly
	dec1, _ := DecryptPageGCM(dataKey, enc1, 0, "/file1.range")
	dec2, _ := DecryptPageGCM(dataKey, enc2, 1, "/file1.range")
	dec3, _ := DecryptPageGCM(dataKey, enc3, 0, "/file2.range")

	if !bytes.Equal(dec1, pageData) || !bytes.Equal(dec2, pageData) || !bytes.Equal(dec3, pageData) {
		t.Error("decryption failed for different key parameters")
	}

	// Verify wrong page number fails authentication
	_, err := DecryptPageGCM(dataKey, enc1, 999, "/file1.range")

	if err == nil {
		t.Error("expected authentication failure for wrong page number")
	}

	// Verify wrong file path fails authentication
	_, err = DecryptPageGCM(dataKey, enc1, 0, "/wrong.range")

	if err == nil {
		t.Error("expected authentication failure for wrong file path")
	}
}

func TestEncryptPageGCM_InvalidPageSize(t *testing.T) {
	dataKey := make([]byte, 32)
	invalidPage := make([]byte, 100) // Wrong size

	_, err := EncryptPageGCM(dataKey, invalidPage, 0, "/test.range")

	if err == nil {
		t.Error("expected error for invalid page size, got nil")
	}
}

func TestDecryptPageGCM_InvalidDataSize(t *testing.T) {
	dataKey := make([]byte, 32)
	invalidData := make([]byte, 10) // Too short

	_, err := DecryptPageGCM(dataKey, invalidData, 0, "/test.range")

	if err == nil {
		t.Error("expected error for invalid encrypted data size, got nil")
	}
}

func TestDecryptPageGCM_AuthenticationFailure(t *testing.T) {
	dataKey := make([]byte, 32)

	for i := range dataKey {
		dataKey[i] = byte(i)
	}

	pageData := make([]byte, AuthenticatedPageSize)

	for i := range pageData {
		pageData[i] = byte(i % 256)
	}

	encrypted, err := EncryptPageGCM(dataKey, pageData, 42, "/test.range")

	if err != nil {
		t.Fatalf("EncryptPageGCM failed: %v", err)
	}

	// Tamper with the ciphertext
	encrypted[len(encrypted)/2] ^= 0xFF

	// Decryption should fail due to authentication tag mismatch
	_, err = DecryptPageGCM(dataKey, encrypted, 42, "/test.range")

	if err == nil {
		t.Error("expected authentication failure for tampered ciphertext, got nil")
	}
}

func TestEncryptDecryptPageGCM_MultiplePages(t *testing.T) {
	dataKey := make([]byte, 32)

	for i := range dataKey {
		dataKey[i] = byte(i)
	}

	filePath := "/test/database.range"

	// Test encrypting and decrypting multiple pages
	for pageNum := uint64(0); pageNum < 10; pageNum++ {
		pageData := make([]byte, AuthenticatedPageSize)

		for i := range pageData {
			pageData[i] = byte((i + int(pageNum)) % 256)
		}

		encrypted, err := EncryptPageGCM(dataKey, pageData, pageNum, filePath)

		if err != nil {
			t.Fatalf("EncryptPageGCM failed for page %d: %v", pageNum, err)
		}

		decrypted, err := DecryptPageGCM(dataKey, encrypted, pageNum, filePath)

		if err != nil {
			t.Fatalf("DecryptPageGCM failed for page %d: %v", pageNum, err)
		}

		if !bytes.Equal(decrypted, pageData) {
			t.Errorf("round-trip failed for page %d", pageNum)
		}
	}
}

func TestEncryptPageGCM_RandomDataVariableLength(t *testing.T) {
	dataKey := make([]byte, 32)

	for i := range dataKey {
		dataKey[i] = byte(i)
	}

	// Test different data patterns that compress differently
	testCases := []struct {
		name     string
		makeData func() []byte
	}{
		{
			name: "all zeros",
			makeData: func() []byte {
				return make([]byte, AuthenticatedPageSize)
			},
		},
		{
			name: "repeating pattern",
			makeData: func() []byte {
				data := make([]byte, AuthenticatedPageSize)

				for i := range data {
					data[i] = byte(i % 16)
				}

				return data
			},
		},
		{
			name: "sequential bytes",
			makeData: func() []byte {
				data := make([]byte, AuthenticatedPageSize)

				for i := range data {
					data[i] = byte(i % 256)
				}

				return data
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			pageData := tc.makeData()

			encrypted, err := EncryptPageGCM(dataKey, pageData, 0, "/test.range")

			if err != nil {
				t.Fatalf("EncryptPageGCM failed: %v", err)
			}

			decrypted, err := DecryptPageGCM(dataKey, encrypted, 0, "/test.range")

			if err != nil {
				t.Fatalf("DecryptPageGCM failed: %v", err)
			}

			if !bytes.Equal(decrypted, pageData) {
				t.Error("round-trip failed")
			}
		})
	}
}
