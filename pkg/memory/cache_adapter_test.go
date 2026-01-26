package memory_test

import (
	"testing"

	"github.com/litebase/litebase/pkg/memory"
)

func TestManagedCache(t *testing.T) {
	t.Run("BasicOperations", func(t *testing.T) {
		mgr, _ := memory.NewManager(memory.Config{Capacity: 10000})

		cache := memory.NewManagedCache(memory.ManagedCacheConfig{
			Capacity:    100,
			Manager:     mgr,
			DefaultSize: 64,
		})

		err := cache.Put("key1", "value1")

		if err != nil {
			t.Fatalf("Failed to put: %v", err)
		}

		value, found := cache.Get("key1")

		if !found {
			t.Error("Expected to find key1")
		}

		if value.(string) != "value1" {
			t.Errorf("Expected 'value1', got '%v'", value)
		}
	})

	t.Run("CustomSizeFunc", func(t *testing.T) {
		mgr, _ := memory.NewManager(memory.Config{Capacity: 10000})

		cache := memory.NewManagedCache(memory.ManagedCacheConfig{
			Capacity: 100,
			Manager:  mgr,
			SizeFunc: func(v any) int64 {
				if str, ok := v.(string); ok {
					return int64(len(str))
				}

				return 64
			},
		})

		err := cache.Put("key1", "hello")

		if err != nil {
			t.Fatalf("Failed to put: %v", err)
		}

		stats := mgr.GetStats()

		if stats.Reserved != 5 {
			t.Errorf("Expected reserved 5, got %d", stats.Reserved)
		}
	})

	t.Run("Delete", func(t *testing.T) {
		mgr, _ := memory.NewManager(memory.Config{Capacity: 10000})

		cache := memory.NewManagedCache(memory.ManagedCacheConfig{
			Capacity:    100,
			Manager:     mgr,
			DefaultSize: 64,
		})

		err := cache.Put("key1", "value1")

		if err != nil {
			t.Fatalf("Failed to put: %v", err)
		}

		cache.Delete("key1")

		_, found := cache.Get("key1")

		if found {
			t.Error("Expected key1 deleted")
		}

		stats := mgr.GetStats()

		if stats.Reserved != 0 {
			t.Errorf("Expected reserved 0, got %d", stats.Reserved)
		}
	})

	t.Run("OutOfMemory", func(t *testing.T) {
		mgr, _ := memory.NewManager(memory.Config{Capacity: 50})

		cache := memory.NewManagedCache(memory.ManagedCacheConfig{
			Capacity: 100,
			Manager:  mgr,
			SizeFunc: func(v any) int64 {
				return 200 // Always return size larger than capacity
			},
		})

		err := cache.Put("key1", "value1")

		if err != memory.ErrNoMemory {
			t.Errorf("Expected ErrNoMemory, got %v", err)
		}
	})
}
