package database_test

import (
	"testing"

	"github.com/litebase/litebase/pkg/database"
)

func TestGetWorkerPool(t *testing.T) {
	t.Run("Singleton", func(t *testing.T) {
		pool1 := database.GetWorkerPool()

		if pool1 == nil {
			t.Fatal("Expected non-nil worker pool")
		}

		pool2 := database.GetWorkerPool()

		if pool1 != pool2 {
			t.Error("GetWorkerPool should return the same instance (singleton)")
		}
	})

	t.Run("WorkerCount", func(t *testing.T) {
		pool := database.GetWorkerPool()

		// Worker pool should have positive workers
		// Can't check maxWorkers as it's private, so just verify we got a pool
		if pool == nil {
			t.Error("Expected non-nil worker pool")
		}
	})
}

func TestShutdownWorkerPool(t *testing.T) {
	t.Run("ShutdownAndRecreate", func(t *testing.T) {
		// Get the pool
		pool1 := database.GetWorkerPool()

		if pool1 == nil {
			t.Fatal("Expected non-nil worker pool")
		}

		// Shutdown
		database.ShutdownWorkerPool()

		// Create new pool
		pool2 := database.GetWorkerPool()

		if pool2 == nil {
			t.Fatal("Expected non-nil worker pool after recreation")
		}

		// Should be a different instance
		if pool1 == pool2 {
			t.Error("Expected different worker pool instance after shutdown")
		}
	})

	t.Run("MultipleShutdowns", func(t *testing.T) {
		// Should not panic on multiple shutdowns
		database.ShutdownWorkerPool()
		database.ShutdownWorkerPool()
		database.ShutdownWorkerPool()
	})
}
