package storage_test

import (
	"os"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/server"
	"github.com/litebase/litebase/pkg/storage"
)

func TestPageLogIndex(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		t.Run("New", func(t *testing.T) {
			pli := storage.NewPageLogIndex(app.Cluster.LocalFS(), "NEW_INDEX")

			if pli == nil {
				t.Fatal("expected non-nil PageLogIndex")
			}
		})

		t.Run("Close", func(t *testing.T) {
			pli := storage.NewPageLogIndex(
				app.Cluster.LocalFS(),
				"INDEX",
			)

			if err := pli.Close(); err != nil {
				t.Fatalf("unexpected error closing PageLogIndex: %v", err)
			}
		})

		t.Run("Delete", func(t *testing.T) {
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

		t.Run("Empty", func(t *testing.T) {
			pli := storage.NewPageLogIndex(app.Cluster.LocalFS(), "EMPTY_INDEX")

			if !pli.Empty() {
				t.Fatal("expected PageLogIndex to be empty")
			}

			// Add an entry to the index
			if err := pli.Put(storage.PageNumber(1), storage.PageVersion(1), 0, []byte{}); err != nil {
				t.Fatalf("unexpected error putting entry: %v", err)
			}

			if pli.Empty() {
				t.Fatal("expected PageLogIndex not to be empty after adding an entry")
			}
		})

		t.Run("File", func(t *testing.T) {
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

		t.Run("Find", func(t *testing.T) {
			pli := storage.NewPageLogIndex(app.Cluster.LocalFS(), "FIND_INDEX")

			// Add some entries
			if err := pli.Put(storage.PageNumber(1), storage.PageVersion(1), 0, []byte{}); err != nil {
				t.Fatalf("unexpected error putting entry: %v", err)
			}

			if err := pli.Put(storage.PageNumber(2), storage.PageVersion(2), 0, []byte{}); err != nil {
				t.Fatalf("unexpected error putting entry: %v", err)
			}

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

		t.Run("Put", func(t *testing.T) {
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

		t.Run("Tombstone", func(t *testing.T) {
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
	})
}
