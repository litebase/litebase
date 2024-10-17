package cluster_test

import (
	"litebase/internal/test"
	"litebase/server"
	"litebase/server/cluster"
	"testing"
)

func TestActivateSignatureHandler(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		currentSignature := app.Config.Signature

		if currentSignature == "test" {
			t.Fatalf("Expected signature to not be 'test'")
		}

		cluster.ActivateSignatureHandler(app.Config, "test")

		if app.Config.Signature != "test" {
			t.Errorf("Expected signature to be 'test', got %s", app.Config.Signature)
		}
	})
}
