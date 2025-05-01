package storage_test

import (
	"context"
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"

	"github.com/litebase/litebase/server/storage"

	"github.com/litebase/litebase/server"
)

func TestNewPageLogManager(t *testing.T) {
	plm := storage.NewPageLogManager(context.Background())
	defer plm.Close()

	if plm == nil {
		t.Fatal("Expected a new PageLogManager instance, got nil")
	}
}

func TestPageLogManager_Close(t *testing.T) {
	plm := storage.NewPageLogManager(context.Background())
	err := plm.Close()

	if err != nil {
		t.Fatalf("Failed to close PageLogManager: %v", err)
	}
}

func TestPageLogManager_Get(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		localFileSystem := app.Cluster.LocalFS()

		plm := storage.NewPageLogManager(context.Background())
		defer plm.Close()

		databaseId := "test_db"
		branchId := "test_branch"

		logger := plm.Get(databaseId, branchId, localFileSystem)

		if logger == nil {
			t.Fatal("Expected to get a PageLogger instance, got nil")
		}

		sameLogger := plm.Get(databaseId, branchId, localFileSystem)

		if logger != sameLogger {
			t.Fatal("Expected to get the same PageLogger instance, got different instances")
		}
	})
}

func TestPageLogManager_Release(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		localFileSystem := app.Cluster.LocalFS()

		plm := storage.NewPageLogManager(context.Background())
		defer plm.Close()

		databaseId := "test_db"
		branchId := "test_branch"

		logger := plm.Get(databaseId, branchId, localFileSystem)

		if logger == nil {
			t.Fatal("Expected to get a PageLogger instance, got nil")
		}

		err := plm.Release(databaseId, branchId)

		if err != nil {
			t.Fatalf("Failed to release pageLogger: %v", err)
		}
	})
}

func TestPageLogManager_SetCompactionFn(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		plm := storage.NewPageLogManager(
			context.Background(),
			func(plm *storage.PageLogManager) {
				plm.CompactionInterval = time.Millisecond * 1
			},
		)

		defer plm.Close()

		compactionCalled := false

		plm.SetCompactionFn(func() {
			compactionCalled = true
		})

		time.Sleep(2 * time.Millisecond)

		if !compactionCalled {
			t.Fatal("Expected compaction function to be called, but it was not")
		}
	})
}
