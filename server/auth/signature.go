package auth

import (
	"fmt"
	"litebase/internal/config"
	"litebase/server/storage"
	"os"
)

func InitSignature() error {
	signature := config.Get().Signature

	storedSignature := storedSignature()

	if signature != "" && storedSignature == "" {
		StoreSignature(signature)
		return nil
	}

	if signature == "" && storedSignature != "" {
		config.Get().Signature = storedSignature

		return nil
	}

	if signature != storedSignature {
		config.Get().SignatureNext = storedSignature

		return nil
	}

	if signature != "" {
		return nil
	}

	return fmt.Errorf("the LITEBASE_SIGNATURE environment variable is not set")
}

func storedSignature() string {
	storedSignature, err := storage.ObjectFS().ReadFile(".signature")

	if err != nil {
		return ""
	}

	return string(storedSignature)
}

func StoreSignature(signature string) error {
	config.Get().Signature = signature
	signaturePath := ".signature"

writeFile:

	err := storage.ObjectFS().WriteFile(signaturePath, []byte(signature), 0644)

	if err != nil {
		if os.IsNotExist(err) {
			err = storage.ObjectFS().MkdirAll("", 0755)

			if err != nil {
				return err
			}

			goto writeFile

		}

		return err
	}

	return nil
}
