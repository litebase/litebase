package storage_test

import (
	"testing"

	"github.com/litebase/litebase/server/storage"
)

func TestNewTieredFileSystemLogger(t *testing.T) {
	dir := t.TempDir()

	logger, err := storage.NewTieredFileSystemLogger(dir)

	if err != nil {
		t.Fatal(err)
	}
	if err != nil {
		t.Fatal(err)
	}

	if logger == nil {
		t.Fatal("logger is nil")
	}
}

func TestNewTieredFileSystemLogger_Close(t *testing.T) {
	dir := t.TempDir()

	logger, err := storage.NewTieredFileSystemLogger(dir)

	if err != nil {
		t.Fatal(err)
	}

	file, err := logger.File()

	if err != nil {
		t.Fatal(err)
	}

	if err := logger.Close(); err != nil {
		t.Fatal(err)
	}

	// Check if the file is closed
	if _, err := file.Write([]byte("test")); err == nil {
		t.Fatal("file is not closed")
	}
}

func TestNewTieredFileSystemLogger_DirtyKeys(t *testing.T) {
	dir := t.TempDir()

	logger, err := storage.NewTieredFileSystemLogger(dir)

	if err != nil {
		t.Fatal(err)
	}

	keys := []string{"key1", "key2", "key3"}
	for _, key := range keys {
		if _, err := logger.Put(key); err != nil {
			t.Fatal(err)
		}
	}

	var dirtyKeys = make(map[string]struct{})
	// Simulate dirty keys
	for key := range logger.DirtyKeys() {
		dirtyKeys[key] = struct{}{}
	}

	if len(dirtyKeys) != len(keys) {
		t.Fatalf("expected %d dirty keys, got %d", len(keys), len(dirtyKeys))
	}

	for _, key := range keys {
		if _, ok := dirtyKeys[key]; !ok {
			t.Fatalf("expected key %s to be in dirty keys", key)
		}
	}
}

func TestNewTieredFileSystemLogger_File(t *testing.T) {
	dir := t.TempDir()

	logger, err := storage.NewTieredFileSystemLogger(dir)

	if err != nil {
		t.Fatal(err)
	}

	file, err := logger.File()

	if err != nil {
		t.Fatal(err)
	}

	if file == nil {
		t.Fatal("file is nil")
	}
}

func TestNewTieredFileSystemLogger_HasDirtyLogs(t *testing.T) {
	dir := t.TempDir()

	logger, err := storage.NewTieredFileSystemLogger(dir)

	if err != nil {
		t.Fatal(err)
	}

	if logger.HasDirtyLogs() {
		t.Fatal("expected HasDirtyLogs to return false")
	}

	_, err = logger.Put("test_key")

	if err != nil {
		t.Fatal(err)
	}

	logger.Close()

	logger, err = storage.NewTieredFileSystemLogger(dir)

	if err != nil {
		t.Fatal(err)
	}

	if !logger.HasDirtyLogs() {
		t.Fatal("expected HasDirtyLogs to return true")
	}
}

func TestNewTieredFileSystemLogger_Put(t *testing.T) {
	dir := t.TempDir()

	logger, err := storage.NewTieredFileSystemLogger(dir)

	if err != nil {
		t.Fatal(err)
	}

	key := "test_key"
	logKey, err := logger.Put(key)

	if err != nil {
		t.Fatal(err)
	}

	if logKey == 0 {
		t.Fatalf("expected logKey to be greater than 0, got %d", logKey)
	}
}

func TestNewTieredFileSystemLogger_Remove(t *testing.T) {
	dir := t.TempDir()

	logger, err := storage.NewTieredFileSystemLogger(dir)

	if err != nil {
		t.Fatal(err)
	}

	key := "test_key"
	logKey, err := logger.Put(key)

	if err != nil {
		t.Fatal(err)
	}

	if logKey == 0 {
		t.Fatalf("expected logKey to be greater than 0, got %d", logKey)
	}

	if err := logger.Remove(key, logKey); err != nil {
		t.Fatal(err)
	}
}

func TestNewTieredFileSystemLogger_Restart(t *testing.T) {
	dir := t.TempDir()

	logger, err := storage.NewTieredFileSystemLogger(dir)

	if err != nil {
		t.Fatal(err)
	}

	logger.Put("test_key")

	if err := logger.Restart(); err != nil {
		t.Fatal(err)
	}

	if logger.HasDirtyLogs() {
		t.Fatal("expected HasDirtyLogs to return false")
	}
}
