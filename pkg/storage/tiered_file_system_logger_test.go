package storage_test

import (
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/server"
	"github.com/litebase/litebase/pkg/storage"
)

func TestTieredFileSystemLogger(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		t.Run("NewTieredFileSystemLogger", func(t *testing.T) {
			logger, err := storage.NewTieredFileSystemLogger(
				app.Cluster.Node().Cluster.LocalFS(),
				"test",
			)

			if err != nil {
				t.Fatal(err)
			}
			if err != nil {
				t.Fatal(err)
			}

			if logger == nil {
				t.Fatal("logger is nil")
			}
		})

		t.Run("Close", func(t *testing.T) {
			logger, err := storage.NewTieredFileSystemLogger(
				app.Cluster.Node().Cluster.LocalFS(),
				"test",
			)

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
		})

		t.Run("DirtyKeys", func(t *testing.T) {
			logger, err := storage.NewTieredFileSystemLogger(
				app.Cluster.Node().Cluster.LocalFS(),
				"test",
			)

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
			for entry := range logger.DirtyKeys() {
				dirtyKeys[entry.Key] = struct{}{}
			}

			if len(dirtyKeys) != len(keys) {
				t.Fatalf("expected %d dirty keys, got %d", len(keys), len(dirtyKeys))
			}

			for _, key := range keys {
				if _, ok := dirtyKeys[key]; !ok {
					t.Fatalf("expected key %s to be in dirty keys", key)
				}
			}
		})

		t.Run("File", func(t *testing.T) {
			logger, err := storage.NewTieredFileSystemLogger(
				app.Cluster.Node().Cluster.LocalFS(),
				"test",
			)

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
		})

		t.Run("HasDirtyLogs", func(t *testing.T) {
			logger, err := storage.NewTieredFileSystemLogger(
				app.Cluster.Node().Cluster.LocalFS(),
				"test_dirty_logs",
			)

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

			logger, err = storage.NewTieredFileSystemLogger(
				app.Cluster.Node().Cluster.LocalFS(),
				"test",
			)

			if err != nil {
				t.Fatal(err)
			}

			if !logger.HasDirtyLogs() {
				t.Fatal("expected HasDirtyLogs to return true")
			}
		})

		t.Run("Put", func(t *testing.T) {
			logger, err := storage.NewTieredFileSystemLogger(
				app.Cluster.Node().Cluster.LocalFS(),
				"test",
			)

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
		})

		t.Run("Remove", func(t *testing.T) {
			logger, err := storage.NewTieredFileSystemLogger(
				app.Cluster.Node().Cluster.LocalFS(),
				"test",
			)

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
		})

		t.Run("Restart", func(t *testing.T) {
			logger, err := storage.NewTieredFileSystemLogger(
				app.Cluster.Node().Cluster.LocalFS(),
				"test",
			)

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
		})
	})
}
