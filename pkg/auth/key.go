package auth

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/litebase/litebase/pkg/config"
	"github.com/litebase/litebase/pkg/storage"
)

func InitKey(c *config.Config, objectFS *storage.FileSystem) error {
	encryptionKey := c.EncryptionKey

	encryptionKeyHash := storedEncryptionKeyHash(objectFS)

	if encryptionKey == "" {
		return fmt.Errorf("the LITEBASE_ENCRYPTION_KEY environment variable is not set")
	}

	if encryptionKeyHash == "" {
		err := StoreEncryptionKey(c, objectFS, encryptionKey)

		if err != nil {
			slog.Error("failed to store encryption key", "error", err)
		}

		return err
	}

	if config.EncryptionKeyHash(encryptionKey) != encryptionKeyHash {
		return nil
	}

	return nil
}

func storedEncryptionKeyHash(objectFS *storage.FileSystem) string {
	storedHash, err := objectFS.ReadFile(".key")

	if err != nil {
		return ""
	}

	return string(storedHash)
}

func StoreEncryptionKey(c *config.Config, objectFS *storage.FileSystem, encryptionKey string) error {
	c.EncryptionKey = encryptionKey
	hashPath := ".key"

writeFile:

	err := objectFS.WriteFile(hashPath, []byte(config.EncryptionKeyHash(encryptionKey)), 0600)

	if err != nil {
		if os.IsNotExist(err) {
			err = objectFS.MkdirAll("", 0750)

			if err != nil {
				return err
			}

			goto writeFile

		}

		return err
	}

	return nil
}
