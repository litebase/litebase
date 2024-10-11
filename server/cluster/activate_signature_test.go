package cluster_test

import (
	"litebase/internal/config"
	"litebase/internal/test"
	"litebase/server/cluster"
	"testing"
)

func TestActivateSignatureHandler(t *testing.T) {
	test.Run(t, func() {
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
