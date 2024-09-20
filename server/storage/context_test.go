package storage_test

import (
	"context"
	"litebase/server/storage"
	"testing"
)

func TestGetStorageContext(t *testing.T) {
	storageContext := storage.GetStorageContext()

	if storageContext == nil {
		t.Errorf("Expected storageContext to not be nil")
	}
}

func TestSetStorageContext(t *testing.T) {
	storageContext := storage.GetStorageContext()

	if storageContext == nil {
		t.Errorf("Expected storageContext to not be nil")
	}

	ctx := context.Background()
	ctx = context.WithValue(ctx, "key", "value")

	storage.SetStorageContext(ctx)

	storageContext = storage.GetStorageContext()

	if storageContext == nil {
		t.Errorf("Expected storageContext to be nil")
	}

	if storageContext.Value("key") != "value" {
		t.Errorf("Expected storageContext to have value 'value'")
	}
}
