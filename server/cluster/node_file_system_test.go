package cluster_test

import (
	"fmt"
	"litebase/internal/test"
	"litebase/server"
	"os"
	"testing"
)

func TestClearFSFiles(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		fs := app.Cluster.TieredFS()

		if fs == nil {
			t.Error("TieredFS() returned nil")
		}

		_, err := fs.Create("test.txt")

		if err != nil {
			t.Error(err)
		}

		_, err = os.Stat(fmt.Sprintf("%s/%s/test.txt", app.Config.DataPath, "tiered"))

		if err != nil {
			t.Error(err)
		}

		app.Cluster.ClearFSFiles()

		if err != nil {
			t.Error(err)
		}

		_, err = os.Stat(fmt.Sprintf("%s/%s/test.txt", app.Config.DataPath, "tiered"))

		if err == nil {
			t.Error("tiered file system files were not cleared")
		}
	})
}

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

func TestTieredFS(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		fs := app.Cluster.TieredFS()

		if fs == nil {
			t.Error("TieredFS() returned nil")
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
