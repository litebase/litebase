package config_test

import (
	"crypto/sha256"
	"encoding/hex"
	"litebase/internal/config"
	"litebase/internal/test"
	"os"
	"testing"
)

func TestInit(t *testing.T) {
	signature := test.CreateHash(32)
	os.Setenv("LITEBASE_DATA_PATH", "../../.test")
	os.Setenv("LITEBASE_SIGNATURE", signature)

	config.Init()

	// The signature file should be stored
	if _, err := os.Stat("../../.test/.signature"); os.IsNotExist(err) {
		t.Fatalf("The signature file was not created")
	}

	test.Teardown()
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

	config.Init()

	// If the signature is not the same as the stored signature, the next signature should be set
	if config.Get().SignatureNext == "" {
		t.Fatalf("The signature next was not set")
	}

	if config.Get().Signature != nextSignature {
		t.Fatalf("The signature was not set")
	}

	test.Teardown()
}

func TestInitWithNoSignature(t *testing.T) {
	os.Setenv("LITEBASE_DATA_PATH", "../../.test")

	// We should get a panic if there is no signature
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("The code did not panic")
		}
	}()

	config.Init()

	test.Teardown()
}

func TestNewConfig(t *testing.T) {
	config.NewConfig()

	if config.Get() == nil {
		t.Fatalf("The config instance was not created")
	}
}

func TestGet(t *testing.T) {
	signature := test.CreateHash(32)
	os.Setenv("LITEBASE_SIGNATURE", signature)
	config.NewConfig()

	if config.Get() == nil {
		t.Fatalf("The config instance was not created")
	}

	if config.Get().Signature != signature {
		t.Fatalf("The signature was not set")
	}

	test.Teardown()
}

func TestSignatureHash(t *testing.T) {
	signature := test.CreateHash(32)
	hash := sha256.Sum256([]byte(signature))

	if config.SignatureHash(signature) != hex.EncodeToString(hash[:]) {
		t.Fatalf("The signature hash was not returned")
	}

	test.Teardown()
}
