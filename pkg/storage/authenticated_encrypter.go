package storage

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/klauspost/compress/s2"
	"golang.org/x/crypto/hkdf"
)

const (
	// GCM nonce size is 12 bytes
	gcmNonceSize = 12
	// GCM auth tag size is 16 bytes
	gcmAuthTagSize = 16
	// AuthenticatedPageSize is the fixed size of unencrypted pages (4096 bytes)
	AuthenticatedPageSize = 4096
)

// derivePageKey derives a unique 32-byte key for a page using HKDF
// Input: dataKey (32 bytes), pageNumber, filePath
// Output: 32-byte key for AES-GCM
func derivePageKey(dataKey []byte, pageNumber uint64, filePath string) ([]byte, error) {
	if len(dataKey) != 32 {
		return nil, fmt.Errorf("dataKey must be 32 bytes, got %d", len(dataKey))
	}

	// Create info from pageNumber || SHA256(filePath)
	filePathHash := sha256.Sum256([]byte(filePath))

	info := make([]byte, 8+32) // pageNumber + filePathHash
	binary.BigEndian.PutUint64(info[0:8], pageNumber)
	copy(info[8:40], filePathHash[:])

	// Derive 32-byte key using HKDF-SHA256
	hkdfReader := hkdf.New(sha256.New, dataKey, nil, info)
	key := make([]byte, 32)

	if _, err := io.ReadFull(hkdfReader, key); err != nil {
		return nil, fmt.Errorf("failed to derive key: %w", err)
	}

	return key, nil
}

// EncryptPageGCM compresses and encrypts a 4096-byte page using AES-256-GCM
// Input: dataKey (32 bytes), pageData (4096 bytes), pageNumber, filePath
// Output: variable-length encrypted data [nonce 12B][ciphertext][authTag 16B]
func EncryptPageGCM(dataKey []byte, pageData []byte, pageNumber uint64, filePath string) ([]byte, error) {
	if len(pageData) != AuthenticatedPageSize {
		return nil, fmt.Errorf("pageData must be %d bytes, got %d", AuthenticatedPageSize, len(pageData))
	}

	// Compress the page data first
	compressed := s2.Encode(nil, pageData)

	// Derive unique key for this page
	pageKey, err := derivePageKey(dataKey, pageNumber, filePath)

	if err != nil {
		return nil, fmt.Errorf("failed to derive page key: %w", err)
	}

	// Create AES cipher
	block, err := aes.NewCipher(pageKey)

	if err != nil {
		return nil, fmt.Errorf("failed to create cipher: %w", err)
	}

	// Create GCM cipher
	aead, err := cipher.NewGCM(block)

	if err != nil {
		return nil, fmt.Errorf("failed to create GCM: %w", err)
	}

	// Generate random nonce
	nonce := make([]byte, gcmNonceSize)

	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("failed to generate nonce: %w", err)
	}

	// Use pageNumber as additional authenticated data (AAD)
	// This protects against block-swap attacks
	aad := make([]byte, 8)
	binary.BigEndian.PutUint64(aad, pageNumber)

	// Encrypt the compressed data
	// GCM automatically appends the 16-byte auth tag to the ciphertext
	ciphertext := aead.Seal(nil, nonce, compressed, aad)

	// Format: [nonce 12B][ciphertext][authTag 16B]
	// The auth tag is already included in ciphertext from aead.Seal
	result := make([]byte, 0, len(nonce)+len(ciphertext))
	result = append(result, nonce...)
	result = append(result, ciphertext...)

	return result, nil
}

// DecryptPageGCM decrypts and decompresses encrypted page data using AES-256-GCM
// Input: dataKey (32 bytes), encryptedData (variable length), pageNumber, filePath
// Output: decrypted and decompressed data (4096 bytes)
func DecryptPageGCM(dataKey []byte, encryptedData []byte, pageNumber uint64, filePath string) ([]byte, error) {
	// Minimum size check: nonce + ciphertext (at least 1 byte) + auth tag
	minSize := gcmNonceSize + 1 + gcmAuthTagSize

	if len(encryptedData) < minSize {
		return nil, fmt.Errorf("encryptedData too short: expected at least %d bytes, got %d", minSize, len(encryptedData))
	}

	// Derive the same key used for encryption
	pageKey, err := derivePageKey(dataKey, pageNumber, filePath)

	if err != nil {
		return nil, fmt.Errorf("failed to derive page key: %w", err)
	}

	// Create AES cipher
	block, err := aes.NewCipher(pageKey)

	if err != nil {
		return nil, fmt.Errorf("failed to create cipher: %w", err)
	}

	// Create GCM cipher
	aead, err := cipher.NewGCM(block)

	if err != nil {
		return nil, fmt.Errorf("failed to create GCM: %w", err)
	}

	// Extract nonce and ciphertext
	nonce := encryptedData[:gcmNonceSize]
	ciphertext := encryptedData[gcmNonceSize:]

	// Use pageNumber as additional authenticated data (AAD)
	aad := make([]byte, 8)
	binary.BigEndian.PutUint64(aad, pageNumber)

	// Decrypt and verify auth tag
	// GCM automatically verifies the auth tag and returns an error if it doesn't match
	compressed, err := aead.Open(nil, nonce, ciphertext, aad)

	if err != nil {
		return nil, fmt.Errorf("failed to decrypt (authentication failed): %w", err)
	}

	// Decompress the data
	decompressed, err := s2.Decode(nil, compressed)

	if err != nil {
		return nil, fmt.Errorf("failed to decompress: %w", err)
	}

	if len(decompressed) != AuthenticatedPageSize {
		return nil, fmt.Errorf("decompressed data wrong size: expected %d bytes, got %d", AuthenticatedPageSize, len(decompressed))
	}

	return decompressed, nil
}
