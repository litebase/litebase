package storage_test

import (
	"os"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/storage"
	"github.com/litebase/litebase/server"
)

func TestNewPageLogIndex(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		pli := storage.NewPageLogIndex(
			app.Cluster.LocalFS(),
			"INDEX",
		)

		if pli == nil {
			t.Fatal("expected non-nil PageLogIndex")
		}
	})
}

func TestPageLogIndex_Close(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		pli := storage.NewPageLogIndex(
			app.Cluster.LocalFS(),
			"INDEX",
		)

		if err := pli.Close(); err != nil {
			t.Fatalf("unexpected error closing PageLogIndex: %v", err)
		}
	})
}

func TestPageLogIndex_Delete(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		pli := storage.NewPageLogIndex(app.Cluster.LocalFS(), "PLI_INDEX")

		if err := pli.Delete(); err != nil {
			t.Fatalf("unexpected error deleting PageLogIndex: %v", err)
		}

		if !pli.Empty() {
			t.Fatal("expected PageLogIndex to be empty after deletion")
		}

		_, err := app.Cluster.LocalFS().Stat("PLI_INDEX")

		if err == nil {
			t.Fatal("expected error when checking deleted PageLogIndex file")
		}

		if !os.IsNotExist(err) {
			t.Fatalf("expected file not to exist, got: %v", err)
		}
	})
}

func TestPageLogIndex_Empty(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		pli := storage.NewPageLogIndex(app.Cluster.LocalFS(), "EMPTY_INDEX")

		if !pli.Empty() {
			t.Fatal("expected PageLogIndex to be empty")
		}

		// Add an entry to the index
		pli.Put(storage.PageNumber(1), storage.PageVersion(1), 0, []byte{})

		if pli.Empty() {
			t.Fatal("expected PageLogIndex not to be empty after adding an entry")
		}
	})
}

func TestPageLogIndex_File(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		pli := storage.NewPageLogIndex(app.Cluster.LocalFS(), "FILE_INDEX")

		file := pli.File()

		if file == nil {
			t.Fatal("expected non-nil file from PageLogIndex")
		}

		if err := file.Close(); err != nil {
			t.Fatalf("unexpected error closing file: %v", err)
		}

		if err := pli.Close(); err == nil {
			t.Fatalf("expected error closing PageLogIndex, got nil")
		}
	})
}

func TestPageLogIndex_Find(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		pli := storage.NewPageLogIndex(app.Cluster.LocalFS(), "FIND_INDEX")

		// Add some entries
		pli.Put(storage.PageNumber(1), storage.PageVersion(1), 0, []byte{})
		pli.Put(storage.PageNumber(2), storage.PageVersion(2), 0, []byte{})

		found, version, offset, err := pli.Find(storage.PageNumber(1), storage.PageVersion(1))

		if err != nil {
			t.Fatalf("unexpected error finding entry: %v", err)
		}

		if !found || version != storage.PageVersion(1) || offset != 0 {
			t.Fatal("expected to find entry with correct version and offset")
		}

		found, version, offset, err = pli.Find(storage.PageNumber(3), storage.PageVersion(1))

		if err != nil {
			t.Fatalf("unexpected error finding non-existent entry: %v", err)
		}

		if found {
			t.Fatal("expected not to find non-existent entry")
		}

		if version != storage.PageVersion(0) || offset != 0 {
			t.Fatal("expected version and offset to be zero for non-existent entry")
		}
	})
}

func TestPageLogIndex_Put(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		pli := storage.NewPageLogIndex(app.Cluster.LocalFS(), "PUT_INDEX")

		err := pli.Put(storage.PageNumber(1), storage.PageVersion(1), 0, []byte("test"))

		if err != nil {
			t.Fatalf("unexpected error putting entry: %v", err)
		}

		found, version, offset, err := pli.Find(storage.PageNumber(1), storage.PageVersion(1))

		if err != nil {
			t.Fatalf("unexpected error finding entry after put: %v", err)
		}

		if !found || version != storage.PageVersion(1) || offset != 0 {
			t.Fatal("expected to find entry with correct version and offset after put")
		}
	})
}

func TestPageLogIndex_Tombstone(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		pli := storage.NewPageLogIndex(app.Cluster.LocalFS(), "TOMBSTONE_INDEX")

		// Add an entry
		err := pli.Put(storage.PageNumber(1), storage.PageVersion(1), 0, []byte("test"))

		if err != nil {
			t.Fatalf("unexpected error putting entry: %v", err)
		}

		// Tombstone the entry
		err = pli.Tombstone(storage.PageNumber(1), storage.PageVersion(1))

		if err != nil {
			t.Fatalf("unexpected error tombstoning entry: %v", err)
		}

		found, version, offset, err := pli.Find(storage.PageNumber(1), storage.PageVersion(1))

		if err != nil {
			t.Fatalf("unexpected error finding tombstoned entry: %v", err)
		}

		if found || version != storage.PageVersion(0) || offset != 0 {
			t.Fatal("expected not to find tombstoned entry")
		}
	})
}
