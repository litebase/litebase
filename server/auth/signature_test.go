package auth_test

import (
	"litebase/internal/config"
	"litebase/internal/test"
	"litebase/server"
	"litebase/server/auth"
	"litebase/server/storage"
	"os"
	"testing"
)

func TestStoreSignature(t *testing.T) {
	test.Run(t, func(app *server.App) {
		config.NewConfig()
		signature := test.CreateHash(64)
		auth.StoreSignature(signature)

		// check if the signature was stored
		if _, err := storage.ObjectFS().Stat(".signature"); os.IsNotExist(err) {
			t.Fatalf("The signature file was not created")
		}

		// check if the signature was stored correctly
		file, err := storage.ObjectFS().Open(".signature")

		if err != nil {
			t.Fatalf("Error opening the signature file: %s", err)
		}

		defer file.Close()

		signatureBytes := make([]byte, 64)

		if _, err := file.Read(signatureBytes); err != nil {
			t.Fatalf("Error reading the signature file: %s", err)
		}

		if string(signatureBytes) != signature {
			t.Fatalf("The signature was not stored correctly")
		}
	})
}
