package auth_test

import (
	"os"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/server"
	"github.com/litebase/litebase/server/auth"
)

func TestStoreSignature(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		signature := test.CreateHash(64)
		auth.StoreSignature(app.Config, app.Cluster.ObjectFS(), signature)

		// check if the signature was stored
		if _, err := app.Cluster.ObjectFS().Stat(".signature"); os.IsNotExist(err) {
			t.Fatalf("The signature file was not created")
		}

		// check if the signature was stored correctly
		file, err := app.Cluster.ObjectFS().Open(".signature")

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
