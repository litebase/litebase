package config_test

import (
	"os"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/config"
)

func TestInit(t *testing.T) {
	encryptionKey := test.CreateHash(32)
	t.Setenv("LITEBASE_DATA_PATH", "../../.test")
	t.Setenv("LITEBASE_ENCRYPTION_KEY", encryptionKey)

	c := config.NewConfig()

	if c == nil {
		t.Fatalf("The config instance was not created")
	}

	test.Teardown(t, "../../.test", nil)
}

func TestInitWithNoEncryptionKey(t *testing.T) {
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
	encryptionKey := test.CreateHash(32)
	os.Setenv("LITEBASE_ENCRYPTION_KEY", encryptionKey)
	c := config.NewConfig()

	if c == nil {
		t.Fatalf("The config instance was not created")
	}

	if c.EncryptionKey != encryptionKey {
		t.Fatalf("The encryption key was not set")
	}

	test.Teardown(t, "../../.test", nil)
}
