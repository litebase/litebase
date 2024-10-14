package cluster_test

import (
	"litebase/internal/config"
	"litebase/internal/test"
	"litebase/server"
	"litebase/server/cluster"
	"testing"
)

func TestActivateSignatureHandler(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		currentSignature := config.Get().Signature

		if currentSignature == "test" {
			t.Fatalf("Expected signature to not be 'test'")
		}

		cluster.ActivateSignatureHandler("test")

		if config.Get().Signature != "test" {
			t.Errorf("Expected signature to be 'test', got %s", config.Get().Signature)
		}
	})
}
