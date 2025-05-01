package config_test

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"testing"

	"github.com/litebase/litebase/common/config"
	"github.com/litebase/litebase/internal/test"
)

func TestInit(t *testing.T) {
	signature := test.CreateHash(32)
	os.Setenv("LITEBASE_DATA_PATH", "../../.test")
	os.Setenv("LITEBASE_SIGNATURE", signature)

	config.NewConfig()

	// The signature file should be stored
	if _, err := os.Stat("../../.test/.signature"); os.IsNotExist(err) {
		t.Fatalf("The signature file was not created")
	}

	test.Teardown(t, "../../.test", nil)
}

func TestInitWithNewSignature(t *testing.T) {
	signature := test.CreateHash(32)
	os.Setenv("LITEBASE_DATA_PATH", "../../.test")
	os.Setenv("LITEBASE_SIGNATURE", signature)

	//Create the signature file
	os.MkdirAll("../../.test", 0755)

	os.WriteFile("../../.test/.signature", []byte(signature), 0644)

	nextSignature := test.CreateHash(32)

	os.Setenv("LITEBASE_SIGNATURE", nextSignature)

	c := config.NewConfig()

	// If the signature is not the same as the stored signature, the next signature should be set
	if c.SignatureNext == "" {
		t.Fatalf("The signature next was not set")
	}

	if c.Signature != nextSignature {
		t.Fatalf("The signature was not set")
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
