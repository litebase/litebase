package cluster_test

import (
	"litebase/internal/test"
	"litebase/server"
	"testing"
)

func TestLocalFS(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		fs := app.Cluster.LocalFS()

		if fs == nil {
			t.Error("LocalFS() returned nil")
		}
	})
}

func TestObjectFS(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		fs := app.Cluster.ObjectFS()

		if fs == nil {
			t.Error("ObjectFS() returned nil")
		}
	})
}

func TestTmpFS(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		fs := app.Cluster.TmpFS()

		if fs == nil {
			t.Error("TmpFS() returned nil")
		}
	})
}

func TestTieredFS(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		fs := app.Cluster.TieredFS()

		if fs == nil {
			t.Error("TieredFS() returned nil")
		}
	})
}
