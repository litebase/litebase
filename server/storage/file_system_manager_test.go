package storage_test

import (
	"litebase/internal/test"
	"litebase/server"
	"litebase/server/storage"
	"testing"
)

func TestLocalFS(t *testing.T) {
	test.Run(t, func(app *server.App) {
		fs := storage.LocalFS()

		if fs == nil {
			t.Error("LocalFS() returned nil")
		}
	})
}

func TestObjectFS(t *testing.T) {
	test.Run(t, func(app *server.App) {
		fs := storage.ObjectFS()

		if fs == nil {
			t.Error("ObjectFS() returned nil")
		}
	})
}

func TestTmpFS(t *testing.T) {
	test.Run(t, func(app *server.App) {
		fs := storage.TmpFS()

		if fs == nil {
			t.Error("TmpFS() returned nil")
		}
	})
}

func TestTieredFS(t *testing.T) {
	test.Run(t, func(app *server.App) {
		fs := storage.TieredFS()

		if fs == nil {
			t.Error("TieredFS() returned nil")
		}
	})
}
