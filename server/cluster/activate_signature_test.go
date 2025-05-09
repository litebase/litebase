package cluster_test

import (
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/server"
	"github.com/litebase/litebase/server/cluster"
)

func TestActivateSignatureHandler(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		currentSignature := app.Config.Signature

		if currentSignature == "test" {
			t.Fatalf("Expected signature to not be 'test'")
		}

		err := cluster.ActivateSignatureHandler(app.Config, 1)

		if err == nil {
			t.Fatalf("Expected error, got nil")
		}

		err = cluster.ActivateSignatureHandler(app.Config, "test")

		if err != nil {
			t.Fatalf("Expected no error, got %s", err)
		}

		if app.Config.Signature != "test" {
			t.Errorf("Expected signature to be 'test', got %s", app.Config.Signature)
		}
	})
}
