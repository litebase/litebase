package auth_test

import (
	"os"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/config"
	"github.com/litebase/litebase/pkg/server"
)

func TestStoreEncryptionKey(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		encryptionKey := test.CreateHash(64)

		err := auth.StoreEncryptionKey(app.Config, app.Cluster.ObjectFS(), encryptionKey)

		if err != nil {
			t.Fatalf("Failed to store encryption key: %v", err)
		}

		// check if the encryption key was stored on object storage
		if _, err := app.Cluster.ObjectFS().Stat(".key"); os.IsNotExist(err) {
			t.Fatalf("The encryption key file was not created")
		}

		// Ensure the encryption key is not stored in network storage
		if _, err := app.Cluster.NetworkFS().Stat(".encryption_key"); !os.IsNotExist(err) {
			t.Fatalf("The encryption key file should not exist in network storage")
		}

		// check if the encryption key was stored correctly
		file, err := app.Cluster.ObjectFS().Open(".key")

		if err != nil {
			t.Fatalf("Error opening the encryption key file: %s", err)
		}

		defer file.Close()

		encryptionKeyBytes := make([]byte, 64)

		if _, err := file.Read(encryptionKeyBytes); err != nil {
			t.Fatalf("Error reading the encryption key file: %s", err)
		}

		if string(encryptionKeyBytes) != config.EncryptionKeyHash(encryptionKey) {
			t.Fatalf("The encryption key was not stored correctly")
		}
	})
}
