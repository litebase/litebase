package storage_test

import (
	"context"
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/memory"
	"github.com/litebase/litebase/pkg/server"
	"github.com/litebase/litebase/pkg/storage"
)

func newTestMemoryManager(t *testing.T) *memory.Manager {
	memManager, err := memory.NewManager(memory.Config{
		Capacity:  100 * 1024 * 1024, // 100MB for tests
		Threshold: 0.85,
	})

	if err != nil {
		t.Fatalf("Failed to create memory manager: %v", err)
	}

	return memManager
}

func TestNewPageLogManager(t *testing.T) {
	plm := storage.NewPageLogManager(context.Background(), newTestMemoryManager(t))

	defer func() {
		if err := plm.Close(); err != nil {
			t.Fatalf("Failed to close PageLogManager: %v", err)
		}
	}()

	if plm == nil {
		t.Fatal("Expected a new PageLogManager instance, got nil")
	}
}

func TestPageLogManager_Close(t *testing.T) {
	plm := storage.NewPageLogManager(context.Background(), newTestMemoryManager(t))
	err := plm.Close()

	if err != nil {
		t.Fatalf("Failed to close PageLogManager: %v", err)
	}
}

func TestPageLogManager_Get(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		tieredFS := app.Cluster.TieredFS()

		plm := storage.NewPageLogManager(context.Background(), app.Cluster.MemoryManager)

		defer func() {
			if err := plm.Close(); err != nil {
				t.Fatalf("Failed to close PageLogManager: %v", err)
			}
		}()

		databaseId := "test_db"
		branchId := "test_branch"

		logger := plm.Get(databaseId, branchId, tieredFS)

		if logger == nil {
			t.Fatal("Expected to get a PageLogger instance, got nil")
		}

		sameLogger := plm.Get(databaseId, branchId, tieredFS)

		if logger != sameLogger {
			t.Fatal("Expected to get the same PageLogger instance, got different instances")
		}
	})
}

func TestPageLogManager_Release(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		tieredFS := app.Cluster.TieredFS()

		plm := storage.NewPageLogManager(context.Background(), app.Cluster.MemoryManager)

		defer func() {
			if err := plm.Close(); err != nil {
				t.Fatalf("Failed to close PageLogManager: %v", err)
			}
		}()

		databaseId := "test_db"
		branchId := "test_branch"

		logger := plm.Get(databaseId, branchId, tieredFS)

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
			app.Cluster.MemoryManager,
			func(plm *storage.PageLogManager) {
				plm.CompactionInterval = time.Millisecond * 1
			},
		)

		defer func() {
			if err := plm.Close(); err != nil {
				t.Error(err)
			}
		}()

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
