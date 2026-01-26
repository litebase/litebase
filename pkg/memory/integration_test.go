package memory_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/litebase/litebase/pkg/memory"
)

func TestIntegration_MultipleComponents(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	mgr, _ := memory.NewManager(memory.Config{
		Capacity:  100 * 1024 * 1024, // 100MB
		Threshold: 0.9,
	})

	// Create multiple components
	cache1 := memory.NewManagedCache(memory.ManagedCacheConfig{
		Capacity:    1000,
		Manager:     mgr,
		DefaultSize: 4096,
		Owner:       "page-cache",
	})

	cache2 := memory.NewManagedCache(memory.ManagedCacheConfig{
		Capacity:    500,
		Manager:     mgr,
		DefaultSize: 1024,
		Owner:       "query-cache",
	})

	bufferPool := memory.NewBufferPool(4096, mgr)

	// Simulate workload
	for i := range 100 {
		err := cache1.Put(fmt.Sprintf("page-%d", i), make([]byte, 4096))

		if err != nil {
			t.Fatalf("Failed to put in cache1: %v", err)
		}

		err = cache2.Put(fmt.Sprintf("query-%d", i), fmt.Sprintf("result-%d", i))

		if err != nil {
			t.Fatalf("Failed to put in cache2: %v", err)
		}

		buf, err := bufferPool.Get()

		if err != nil {
			continue
		}

		err = bufferPool.Put(buf)

		if err != nil {
			t.Fatalf("Failed to put buffer: %v", err)
		}
	}

	stats := mgr.GetStats()

	t.Logf("Reserved: %d, Available: %d, Utilization: %.2f%%",
		stats.Reserved, stats.Available, stats.UtilizationPercent)

	if stats.Reserved > stats.Capacity {
		t.Errorf("Memory leak: reserved %d > capacity %d", stats.Reserved, stats.Capacity)
	}
}

func TestStress_ConcurrentAccess(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	mgr, _ := memory.NewManager(memory.Config{
		Capacity:  50 * 1024 * 1024, // 50MB
		Threshold: 0.85,
	})

	const numGoroutines = 50
	const operationsPerGoroutine = 1000

	wg := sync.WaitGroup{}
	wg.Add(numGoroutines)

	for g := range numGoroutines {
		go func(id int) {
			defer wg.Done()

			cache := memory.NewManagedCache(memory.ManagedCacheConfig{
				Capacity:    100,
				Manager:     mgr,
				DefaultSize: 512,
				Owner:       fmt.Sprintf("goroutine-%d", id),
			})

			for i := range operationsPerGoroutine {
				key := fmt.Sprintf("key-%d-%d", id, i)

				err := cache.Put(key, make([]byte, 512))

				if err != nil {
					continue
				}

				cache.Get(key)

				if i%10 == 0 {
					cache.Delete(key)
				}
			}
		}(g)
	}

	wg.Wait()

	stats := mgr.GetStats()

	t.Logf("Final reserved: %d bytes (%.2f%% utilization)",
		stats.Reserved, stats.UtilizationPercent)

	if stats.Reserved > stats.Capacity {
		t.Errorf("Memory leak: reserved %d > capacity %d", stats.Reserved, stats.Capacity)
	}
}

func BenchmarkManager_Throughput(b *testing.B) {
	mgr, _ := memory.NewManager(memory.Config{
		Capacity: 1024 * 1024 * 1024, // 1GB
	})

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			lease, err := mgr.Request(4096)

			if err != nil {
				b.Fatalf("Failed to allocate: %v", err)
			}

			err = mgr.Release(lease)

			if err != nil {
				b.Fatalf("Failed to release: %v", err)
			}
		}
	})
}
