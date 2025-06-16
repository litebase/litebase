package config_test

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/config"
)

func TestInit(t *testing.T) {
	signature := test.CreateHash(32)
	t.Setenv("LITEBASE_DATA_PATH", "../../.test")
	t.Setenv("LITEBASE_SIGNATURE", signature)

	c := config.NewConfig()

	if c == nil {
		t.Fatalf("The config instance was not created")
	}

	test.Teardown(t, "../../.test", nil)
}

func TestInitWithNoSignature(t *testing.T) {
	os.Setenv("LITEBASE_DATA_PATH", "../../.test")

	config.NewConfig()

	test.Teardown(t, "../../.test", nil)
}

func TestNewConfig(t *testing.T) {
	c := config.NewConfig()

	if c == nil {
		t.Fatalf("The config instance was not created")
	}
}

func TestGet(t *testing.T) {
	signature := test.CreateHash(32)
	os.Setenv("LITEBASE_SIGNATURE", signature)
	c := config.NewConfig()

	if c == nil {
		t.Fatalf("The config instance was not created")
	}

	if c.Signature != signature {
		t.Fatalf("The signature was not set")
	}

	test.Teardown(t, "../../.test", nil)
}

func TestSignatureHash(t *testing.T) {
	signature := test.CreateHash(32)
	hash := sha256.Sum256([]byte(signature))

	if config.SignatureHash(signature) != hex.EncodeToString(hash[:]) {
		t.Fatalf("The signature hash was not returned")
	}

	test.Teardown(t, "../../.test", nil)
}
