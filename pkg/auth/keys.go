package auth

import (
	"log"

	"github.com/litebase/litebase/pkg/storage"

	"github.com/litebase/litebase/pkg/config"
)

/*
This method is used to retrieve the active key that is based on the encryption key
stored in shared file storage. Since this method depends on file i/o, it is not
recommended to use this method in a loop or in a hot path.
*/
func ActiveEncryptionKey(c *config.Config) string {
	return c.EncryptionKey
}

func ActiveEncryptionKeyHash(c *config.Config) string {
	return EncryptionKeyHash(ActiveEncryptionKey(c))
}

func AllKeys(objectFs *storage.FileSystem) map[string]string {
	var keys = map[string]string{}

	// TODO: ignore directories that start with an underscore
	keyFiles, err := objectFs.ReadDir("/")

	if err != nil {
		log.Println("Error reading keys", err)
		return keys
	}

	for _, keyFile := range keyFiles {
		// Ignore non directories
		if !keyFile.IsDir() {
			continue
		}

		//Ignore paths that start with an underscore
		if keyFile.Name()[0] == '_' {
			continue
		}

		keyHash := EncryptionKeyHash(keyFile.Name())
		keys[keyHash] = keyFile.Name()
	}

	return keys
}

func FindKey(objectFs *storage.FileSystem, hash string) string {
	keys := AllKeys(objectFs)

	if _, ok := keys[hash]; ok {
		return keys[hash]
	}

	return ""
}
