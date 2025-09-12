package auth

import (
	"fmt"

	"github.com/litebase/litebase/pkg/config"
	"github.com/litebase/litebase/pkg/storage"
)

// ValidateEncryptionKey validates that the provided encryption key matches
// the checksum of the last stored encryption key in the .key file
func ValidateEncryptionKey(encryptionKey string, objectFS *storage.FileSystem) error {
	if encryptionKey == "" {
		return fmt.Errorf("encryption key cannot be empty")
	}

	// Get the stored encryption key hash from the .key file
	storedHash := storedEncryptionKeyHash(objectFS)

	if storedHash == "" {
		return fmt.Errorf("no stored encryption key hash found in .key file")
	}

	// Calculate the hash of the provided encryption key
	providedKeyHash := EncryptionKeyHash(encryptionKey)

	// Compare the hashes
	if providedKeyHash != storedHash {
		return fmt.Errorf("provided encryption key does not match the stored key checksum")
	}

	return nil
}

// ValidateEncryptionKeyWithConfig validates the encryption key using the config and object filesystem
func ValidateEncryptionKeyWithConfig(c *config.Config, objectFS *storage.FileSystem) error {
	return ValidateEncryptionKey(c.EncryptionKey, objectFS)
}
