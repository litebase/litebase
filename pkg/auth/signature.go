package auth

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/litebase/litebase/pkg/config"
	"github.com/litebase/litebase/pkg/storage"
)

func InitSignature(c *config.Config, objectFS *storage.FileSystem) error {
	signature := c.Signature

	storedSignature := storedSignature(objectFS)

	if signature == "" {
		return fmt.Errorf("the LITEBASE_SIGNATURE environment variable is not set")
	}

	if storedSignature == "" {
		err := StoreSignature(c, objectFS, signature)

		if err != nil {
			slog.Error("failed to store signature", "error", err)
		}

		return err
	}

	if config.SignatureHash(signature) != storedSignature {
		return nil
	}

	return nil
}

func storedSignature(objectFS *storage.FileSystem) string {
	storedSignature, err := objectFS.ReadFile(".signature")

	if err != nil {
		return ""
	}

	return string(storedSignature)
}

func StoreSignature(c *config.Config, objectFS *storage.FileSystem, signature string) error {
	c.Signature = signature
	signaturePath := ".signature"

writeFile:

	err := objectFS.WriteFile(signaturePath, []byte(config.SignatureHash(signature)), 0600)

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
