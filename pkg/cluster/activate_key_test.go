package cluster_test

import (
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/cluster"
	"github.com/litebase/litebase/pkg/server"
)

func TestActivateKeyHandler(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		currentKey := app.Config.EncryptionKey

		if currentKey == "test" {
			t.Fatalf("Expected key to not be 'test'")
		}

		err := cluster.ActivateKeyHandler(app.Config, 1)

		if err == nil {
			t.Fatalf("Expected error, got nil")
		}

		err = cluster.ActivateKeyHandler(app.Config, "test")

		if err != nil {
			t.Fatalf("Expected no error, got %s", err)
		}

		if app.Config.EncryptionKey != "test" {
			t.Errorf("Expected key to be 'test', got %s", app.Config.EncryptionKey)
		}
	})
}
