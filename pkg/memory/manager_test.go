package memory_test

import (
	"sync"
	"testing"
	"time"

	"github.com/litebase/litebase/pkg/memory"
)

func TestManager(t *testing.T) {
	t.Run("BasicAllocation", func(t *testing.T) {
		mgr, err := memory.NewManager(memory.Config{Capacity: 10000})

		if err != nil {
			t.Fatalf("Failed to create manager: %v", err)
		}

		lease, err := mgr.Request(1024)

		if err != nil {
			t.Fatalf("Failed to request: %v", err)
		}

		if lease.Size != 1024 {
			t.Errorf("Expected size 1024, got %d", lease.Size)
		}
	})

	t.Run("Release", func(t *testing.T) {
		mgr, err := memory.NewManager(memory.Config{Capacity: 10000})

		if err != nil {
			t.Fatalf("Failed to create manager: %v", err)
		}

		lease, err := mgr.Request(1024)

		if err != nil {
			t.Fatalf("Failed to request: %v", err)
		}

		err = mgr.Release(lease)

		if err != nil {
			t.Fatalf("Failed to release: %v", err)
		}

		stats := mgr.GetStats()

		if stats.Reserved != 0 {
			t.Errorf("Expected reserved 0, got %d", stats.Reserved)
		}
	})

	t.Run("OutOfMemory", func(t *testing.T) {
		mgr, err := memory.NewManager(memory.Config{Capacity: 100})

		if err != nil {
			t.Fatalf("Failed to create manager: %v", err)
		}

		_, err = mgr.Request(200)

		if err != memory.ErrNoMemory {
			t.Errorf("Expected ErrNoMemory, got %v", err)
		}
	})

	t.Run("Concurrent", func(t *testing.T) {
		mgr, err := memory.NewManager(memory.Config{Capacity: 100000})

		if err != nil {
			t.Fatalf("Failed to create manager: %v", err)
		}

		wg := sync.WaitGroup{}

		for range 50 {
			wg.Go(func() {
				lease, err := mgr.Request(1024)

				if err != nil {
					return
				}

				time.Sleep(10 * time.Millisecond)

				err = mgr.Release(lease)

				if err != nil {
					t.Errorf("Failed to release: %v", err)
				}
			})
		}

		wg.Wait()

		stats := mgr.GetStats()

		if stats.Reserved != 0 {
			t.Errorf("Expected all released, got %d", stats.Reserved)
		}
	})

	t.Run("Eviction", func(t *testing.T) {
		mgr, err := memory.NewManager(memory.Config{Capacity: 1000, Threshold: 0.9})

		if err != nil {
			t.Fatalf("Failed to create manager: %v", err)
		}

		lease1, err := mgr.Request(800, memory.Reclaimable(true))

		if err != nil {
			t.Fatalf("Failed to request first lease: %v", err)
		}

		lease2, err := mgr.Request(500, memory.Reclaimable(false))

		if err != nil {
			t.Logf("Request failed: %v", err)
		} else {
			if !lease1.Reclaimed {
				t.Error("Expected first lease reclaimed")
			}

			err = mgr.Release(lease2)

			if err != nil {
				t.Errorf("Failed to release: %v", err)
			}
		}
	})
}
