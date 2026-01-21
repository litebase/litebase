package storage

import (
	"bytes"
	"crypto/sha256"
	"testing"
)

func TestDerivePageIV(t *testing.T) {
	dataKey := make([]byte, 32)

	for i := range dataKey {
		dataKey[i] = byte(i)
	}

	tests := []struct {
		name       string
		pageNumber uint64
		timestamp  int64
		filePath   string
	}{
		{"page 0, timestamp 0", 0, 0, "/path/to/file.wal"},
		{"page 1, timestamp 0", 1, 0, "/path/to/file.wal"},
		{"page 0, timestamp 1", 0, 1, "/path/to/file.wal"},
		{"page 0, different path", 0, 0, "/different/path.wal"},
	}

	// Collect all IVs to verify uniqueness
	ivs := make(map[string]bool)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			iv, err := derivePageIV(dataKey, tt.pageNumber, tt.timestamp, tt.filePath)

			if err != nil {
				t.Fatalf("derivePageIV failed: %v", err)
			}

			if len(iv) != 16 {
				t.Errorf("expected IV length 16, got %d", len(iv))
			}

			// Check for uniqueness
			ivStr := string(iv)

			if ivs[ivStr] {
				t.Errorf("IV collision detected for %s", tt.name)
			}

			ivs[ivStr] = true
		})
	}

	// Verify we generated 4 unique IVs
	if len(ivs) != len(tests) {
		t.Errorf("expected %d unique IVs, got %d", len(tests), len(ivs))
	}
}

func TestDerivePageIV_InvalidKeySize(t *testing.T) {
	invalidKey := make([]byte, 16) // Wrong size

	_, err := derivePageIV(invalidKey, 0, 0, "/path/to/file.wal")

	if err == nil {
		t.Error("expected error for invalid key size, got nil")
	}
}

func TestDerivePageIV_Deterministic(t *testing.T) {
	dataKey := make([]byte, 32)

	for i := range dataKey {
		dataKey[i] = byte(i)
	}

	// Derive IV twice with same parameters
	iv1, err := derivePageIV(dataKey, 42, 123456, "/test/file.wal")

	if err != nil {
		t.Fatalf("first derivePageIV failed: %v", err)
	}

	iv2, err := derivePageIV(dataKey, 42, 123456, "/test/file.wal")

	if err != nil {
		t.Fatalf("second derivePageIV failed: %v", err)
	}

	if !bytes.Equal(iv1, iv2) {
		t.Error("derivePageIV is not deterministic")
	}
}

func TestStreamHeader_WriteRead(t *testing.T) {
	keyHash := sha256.Sum256([]byte("test-key"))

	// Write header
	buf := &bytes.Buffer{}
	err := WriteStreamHeader(buf, keyHash)

	if err != nil {
		t.Fatalf("WriteStreamHeader failed: %v", err)
	}

	if buf.Len() != StreamHeaderSize {
		t.Errorf("expected header size %d, got %d", StreamHeaderSize, buf.Len())
	}

	// Read header
	header, err := ReadStreamHeader(buf)

	if err != nil {
		t.Fatalf("ReadStreamHeader failed: %v", err)
	}

	if string(header.Magic[:]) != StreamHeaderMagic {
		t.Errorf("expected magic %q, got %q", StreamHeaderMagic, string(header.Magic[:]))
	}

	if header.Version != StreamHeaderVersion {
		t.Errorf("expected version %d, got %d", StreamHeaderVersion, header.Version)
	}

	if !bytes.Equal(header.KeyHash[:], keyHash[:]) {
		t.Error("key hash mismatch")
	}
}

func TestStreamHeader_InvalidMagic(t *testing.T) {
	buf := bytes.NewBuffer([]byte("BADMAGIC" + string(make([]byte, 56))))

	_, err := ReadStreamHeader(buf)

	if err == nil {
		t.Error("expected error for invalid magic, got nil")
	}
}

func TestStreamHeader_InvalidVersion(t *testing.T) {
	buf := &bytes.Buffer{}

	// Write header with invalid version
	buf.WriteString(StreamHeaderMagic)
	buf.WriteByte(99) // Invalid version
	buf.Write(make([]byte, 59))

	_, err := ReadStreamHeader(buf)

	if err == nil {
		t.Error("expected error for invalid version, got nil")
	}
}

func TestEncryptDecryptPageCTR_RoundTrip(t *testing.T) {
	dataKey := make([]byte, 32)

	for i := range dataKey {
		dataKey[i] = byte(i)
	}

	pageData := make([]byte, StreamPageSize)

	for i := range pageData {
		pageData[i] = byte(i % 256)
	}

	pageNumber := uint64(42)
	timestamp := int64(123456789)
	filePath := "/test/database.wal"

	// Encrypt
	encrypted, err := EncryptPageCTR(dataKey, pageData, pageNumber, timestamp, filePath)

	if err != nil {
		t.Fatalf("EncryptPageCTR failed: %v", err)
	}

	if len(encrypted) != StreamPageSize {
		t.Errorf("expected encrypted size %d, got %d", StreamPageSize, len(encrypted))
	}

	// Verify encrypted data is different from plaintext
	if bytes.Equal(encrypted, pageData) {
		t.Error("encrypted data should not match plaintext")
	}

	// Decrypt
	decrypted, err := DecryptPageCTR(dataKey, encrypted, pageNumber, timestamp, filePath)

	if err != nil {
		t.Fatalf("DecryptPageCTR failed: %v", err)
	}

	if len(decrypted) != StreamPageSize {
		t.Errorf("expected decrypted size %d, got %d", StreamPageSize, len(decrypted))
	}

	// Verify round-trip
	if !bytes.Equal(decrypted, pageData) {
		t.Error("decrypted data does not match original plaintext")
	}
}

func TestEncryptPageCTR_DifferentIVsProduceDifferentCiphertext(t *testing.T) {
	dataKey := make([]byte, 32)

	for i := range dataKey {
		dataKey[i] = byte(i)
	}

	pageData := make([]byte, StreamPageSize)

	for i := range pageData {
		pageData[i] = byte(i % 256)
	}

	// Encrypt same page with different parameters
	enc1, _ := EncryptPageCTR(dataKey, pageData, 0, 0, "/file1.wal")
	enc2, _ := EncryptPageCTR(dataKey, pageData, 1, 0, "/file1.wal") // Different page
	enc3, _ := EncryptPageCTR(dataKey, pageData, 0, 1, "/file1.wal") // Different timestamp
	enc4, _ := EncryptPageCTR(dataKey, pageData, 0, 0, "/file2.wal") // Different path

	// All should be different
	if bytes.Equal(enc1, enc2) {
		t.Error("different page numbers should produce different ciphertext")
	}

	if bytes.Equal(enc1, enc3) {
		t.Error("different timestamps should produce different ciphertext")
	}

	if bytes.Equal(enc1, enc4) {
		t.Error("different file paths should produce different ciphertext")
	}
}

func TestEncryptPageCTR_InvalidPageSize(t *testing.T) {
	dataKey := make([]byte, 32)
	invalidPage := make([]byte, 100) // Wrong size

	_, err := EncryptPageCTR(dataKey, invalidPage, 0, 0, "/test.wal")

	if err == nil {
		t.Error("expected error for invalid page size, got nil")
	}
}

func TestDecryptPageCTR_InvalidPageSize(t *testing.T) {
	dataKey := make([]byte, 32)
	invalidData := make([]byte, 100) // Wrong size

	_, err := DecryptPageCTR(dataKey, invalidData, 0, 0, "/test.wal")

	if err == nil {
		t.Error("expected error for invalid encrypted data size, got nil")
	}
}

func TestEncryptDecryptPageCTR_MultiplePages(t *testing.T) {
	dataKey := make([]byte, 32)

	for i := range dataKey {
		dataKey[i] = byte(i)
	}

	timestamp := int64(123456789)
	filePath := "/test/database.wal"

	// Test encrypting and decrypting multiple pages
	for pageNum := range uint64(10) {
		pageData := make([]byte, StreamPageSize)

		for i := range pageData {
			pageData[i] = byte((i + int(pageNum)) % 256)
		}

		encrypted, err := EncryptPageCTR(dataKey, pageData, pageNum, timestamp, filePath)

		if err != nil {
			t.Fatalf("EncryptPageCTR failed for page %d: %v", pageNum, err)
		}

		decrypted, err := DecryptPageCTR(dataKey, encrypted, pageNum, timestamp, filePath)

		if err != nil {
			t.Fatalf("DecryptPageCTR failed for page %d: %v", pageNum, err)
		}

		if !bytes.Equal(decrypted, pageData) {
			t.Errorf("round-trip failed for page %d", pageNum)
		}
	}
}
