package storage_test

import (
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/storage"
	"github.com/litebase/litebase/server"
)

func TestNewPageLoggerIndex(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		pli, err := storage.NewPageLoggerIndex(
			app.Cluster.NetworkFS(),
			"INDEX",
		)

		if err != nil {
			t.Fatal("expected no error, got:", err)
		}

		if pli == nil {
			t.Fatal("expected non-nil PageLoggerIndex")
		}
	})
}

func TestPageLoggerIndex_Close(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		pli, err := storage.NewPageLoggerIndex(
			app.Cluster.NetworkFS(),
			"INDEX",
		)

		if err != nil {
			t.Fatal("expected no error, got:", err)
		}

		if err := pli.Close(); err != nil {
			t.Fatalf("unexpected error closing PageLoggerIndex: %v", err)
		}
	})
}

func TestPageLoggerIndex_FileName(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		pli, err := storage.NewPageLoggerIndex(
			app.Cluster.NetworkFS(),
			"PLI_INDEX",
		)

		if err != nil {
			t.Fatal("expected no error, got:", err)
		}

		file := pli.File()

		if file == nil {
			t.Fatal("expected non-nil file, got nil")
		}

		if err := file.Close(); err != nil {
			t.Fatalf("unexpected error closing file: %v", err)
		}
	})
}

func TestPageLoggerIndex_Find(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		pli, err := storage.NewPageLoggerIndex(
			app.Cluster.NetworkFS(),
			"FIND_INDEX",
		)

		if err != nil {
			t.Fatal("expected no error, got:", err)
		}

		err = pli.Push(storage.PageGroup(1), storage.PageNumber(1), storage.PageGroupVersion(1))

		if err != nil {
			t.Fatalf("unexpected error pushing page: %v", err)
		}

		_, found, err := pli.Find(storage.PageGroup(1), storage.PageNumber(1), storage.PageVersion(1))

		if err != nil {
			t.Fatalf("unexpected error finding page: %v", err)
		}

		if !found {
			t.Fatal("expected page to be found")
		}
	})
}

func TestPageLogIndex_Push(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		pli, err := storage.NewPageLoggerIndex(
			app.Cluster.NetworkFS(),
			"INDEX",
		)

		if err != nil {
			t.Fatal("expected no error, got:", err)
		}

		err = pli.Push(storage.PageGroup(1), storage.PageNumber(1), storage.PageGroupVersion(1))

		if err != nil {
			t.Fatalf("unexpected error pushing page: %v", err)
		}
	})
}
