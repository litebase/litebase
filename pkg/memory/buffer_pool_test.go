package memory_test

import (
	"testing"

	"github.com/litebase/litebase/pkg/memory"
)

func TestBufferPool(t *testing.T) {
	t.Run("GetAndPut", func(t *testing.T) {
		mgr, _ := memory.NewManager(memory.Config{Capacity: 10000})
		pool := memory.NewBufferPool(4096, mgr)

		buf, err := pool.Get()

		if err != nil {
			t.Fatalf("Failed to get: %v", err)
		}

		if len(*buf) != 4096 {
			t.Errorf("Expected size 4096, got %d", len(*buf))
		}

		stats := mgr.GetStats()

		if stats.Reserved != 4096 {
			t.Errorf("Expected reserved 4096, got %d", stats.Reserved)
		}

		err = pool.Put(buf)

		if err != nil {
			t.Fatalf("Failed to put: %v", err)
		}

		stats = mgr.GetStats()

		if stats.Reserved != 0 {
			t.Errorf("Expected reserved 0, got %d", stats.Reserved)
		}
	})

	t.Run("OutOfMemory", func(t *testing.T) {
		mgr, _ := memory.NewManager(memory.Config{Capacity: 4000})
		pool := memory.NewBufferPool(4096, mgr)

		_, err := pool.Get()

		if err != memory.ErrNoMemory {
			t.Errorf("Expected ErrNoMemory, got %v", err)
		}
	})
}

func TestBytesBufferPool(t *testing.T) {
	t.Run("GetAndPut", func(t *testing.T) {
		mgr, _ := memory.NewManager(memory.Config{Capacity: 10000})
		pool := memory.NewBytesBufferPool(1024, mgr)

		buf, err := pool.Get()

		if err != nil {
			t.Fatalf("Failed to get: %v", err)
		}

		buf.WriteString("test")

		if buf.String() != "test" {
			t.Errorf("Expected 'test', got '%s'", buf.String())
		}

		err = pool.Put(buf)

		if err != nil {
			t.Fatalf("Failed to put: %v", err)
		}

		buf2, err := pool.Get()

		if err != nil {
			t.Fatalf("Failed to get: %v", err)
		}

		if buf2.Len() != 0 {
			t.Errorf("Expected reset buffer, got length %d", buf2.Len())
		}

		err = pool.Put(buf2)

		if err != nil {
			t.Fatalf("Failed to put: %v", err)
		}
	})
}
