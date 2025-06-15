package auth

import (
	"log"

	"github.com/litebase/litebase/pkg/storage"

	"github.com/litebase/litebase/pkg/config"
)

/*
This method is used to retrieve the active signature that is based on the signature
stored in shared file storage. Since this method depends on file i/o, it is not
recommended to use this method in a loop or in a hot path.
*/
func ActiveSignature(c *config.Config) string {
	return c.Signature
}

func ActiveSignatureHash(c *config.Config) string {
	return SignatureHash(ActiveSignature(c))
}

func AllSignatures(objectFs *storage.FileSystem) map[string]string {
	var signatures = map[string]string{}

	// TODO: ignore directories that start with an underscore
	signatureFiles, err := objectFs.ReadDir("/")

	if err != nil {
		log.Println("Error reading signatures", err)
		return signatures
	}

	for _, signatureFile := range signatureFiles {
		// Ignore non directories
		if !signatureFile.IsDir() {
			continue
		}

		//Ignore paths that start with an underscore
		if signatureFile.Name()[0] == '_' {
			continue
		}

		signatureHash := SignatureHash(signatureFile.Name())
		signatures[signatureHash] = signatureFile.Name()
	}

	return signatures
}

func FindSignature(objectFs *storage.FileSystem, hash string) string {
	signatures := AllSignatures(objectFs)

	if _, ok := signatures[hash]; ok {
		return signatures[hash]
	}

	return ""
}
