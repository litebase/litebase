package auth

import (
	"litebase/internal/config"
	"litebase/server/storage"

	"strings"
)

/*
This method is used to retrieve the active signature that is based on the signature
stored in shared file storage. Since this method depends on file i/o, it is not
recommended to use this method in a loop or in a hot path.
*/
func ActiveSignature() string {
	return config.Get().Signature
}

func ActiveSignatureHash() string {
	return SignatureHash(ActiveSignature())
}

func AllSignatures() map[string]string {
	var signatures = map[string]string{}

	directoryPath := strings.Join([]string{
		config.Get().DataPath,
		".litebase",
	}, "/")

	signatureFiles, err := storage.FS().ReadDir(directoryPath)

	if err != nil {
		return signatures
	}

	for _, signatureFile := range signatureFiles {
		if !signatureFile.IsDir {
			continue
		}

		signatureHash := SignatureHash(signatureFile.Name)
		signatures[signatureHash] = signatureFile.Name
	}

	return signatures
}

func FindSignature(hash string) string {
	signatures := AllSignatures()

	if _, ok := signatures[hash]; ok {
		return signatures[hash]
	}

	return ""
}
