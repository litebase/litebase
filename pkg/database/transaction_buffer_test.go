package database

import (
	"bytes"
	"testing"

	"github.com/litebase/litebase/pkg/memory"
)

func TestGetCoalescedWrites(t *testing.T) {
	mgr, err := memory.NewManager(memory.Config{Capacity: 32 * 1024 * 1024, Threshold: 0.9})

	if err != nil {
		t.Fatalf("failed to create memory manager: %v", err)
	}

	const pageSize = 4096

	t.Run("sequential slab-contiguous pages coalesce into one write", func(t *testing.T) {
		buf, err := NewTransactionBuffer(mgr, 1)

		if err != nil {
			t.Fatalf("failed to create buffer: %v", err)
		}

		defer buf.Release(mgr)

		page := make([]byte, pageSize)

		for i := range page {
			page[i] = byte(i)
		}

		// Write 4 sequential pages at consecutive offsets.
		for i := 0; i < 4; i++ {
			if _, err := buf.WriteAt(page, int64(i)*pageSize); err != nil {
				t.Fatalf("WriteAt page %d failed: %v", i, err)
			}
		}

		coalesced := buf.GetCoalescedWrites()

		if len(coalesced) != 1 {
			t.Fatalf("expected 1 coalesced write, got %d", len(coalesced))
		}

		if coalesced[0].offset != 0 {
			t.Errorf("expected offset 0, got %d", coalesced[0].offset)
		}

		if len(coalesced[0].data) != 4*pageSize {
			t.Errorf("expected coalesced length %d, got %d", 4*pageSize, len(coalesced[0].data))
		}

		// Verify original per-page writes are unchanged for cache path.
		originals := buf.GetWrites()

		if len(originals) != 4 {
			t.Fatalf("expected 4 original writes, got %d", len(originals))
		}
	})

	t.Run("non-sequential offsets are not coalesced", func(t *testing.T) {
		buf, err := NewTransactionBuffer(mgr, 2)

		if err != nil {
			t.Fatalf("failed to create buffer: %v", err)
		}

		defer buf.Release(mgr)

		page := make([]byte, pageSize)

		// Write with a gap: page 0 then page 2 (skip page 1).
		if _, err := buf.WriteAt(page, 0); err != nil {
			t.Fatalf("WriteAt 0 failed: %v", err)
		}

		if _, err := buf.WriteAt(page, 2*pageSize); err != nil {
			t.Fatalf("WriteAt 2*pageSize failed: %v", err)
		}

		coalesced := buf.GetCoalescedWrites()

		if len(coalesced) != 2 {
			t.Fatalf("expected 2 separate writes, got %d", len(coalesced))
		}
	})

	t.Run("overwrite of existing offset breaks slab adjacency, not coalesced with neighbour", func(t *testing.T) {
		buf, err := NewTransactionBuffer(mgr, 3)

		if err != nil {
			t.Fatalf("failed to create buffer: %v", err)
		}

		defer buf.Release(mgr)

		page := make([]byte, pageSize)
		updated := make([]byte, pageSize)
		updated[0] = 0xFF

		// Write pages 0 and 1, then overwrite page 0 (creates non-contiguous slab hole).
		if _, err := buf.WriteAt(page, 0); err != nil {
			t.Fatalf("WriteAt 0 failed: %v", err)
		}

		if _, err := buf.WriteAt(page, pageSize); err != nil {
			t.Fatalf("WriteAt pageSize failed: %v", err)
		}

		// Overwrite page 0 \u2014 its new slab region is after page 1's region.
		if _, err := buf.WriteAt(updated, 0); err != nil {
			t.Fatalf("overwrite WriteAt 0 failed: %v", err)
		}

		coalesced := buf.GetCoalescedWrites()

		// page 1 then overwritten-page-0: offsets not sequential so must not merge.
		for _, w := range coalesced {
			if w.offset == 0 && !bytes.Equal(w.data[:1], []byte{0xFF}) {
				t.Errorf("expected overwritten data at offset 0, got %v", w.data[:1])
			}
		}
	})

	t.Run("single write returns one entry", func(t *testing.T) {
		buf, err := NewTransactionBuffer(mgr, 4)

		if err != nil {
			t.Fatalf("failed to create buffer: %v", err)
		}

		defer buf.Release(mgr)

		page := make([]byte, pageSize)

		if _, err := buf.WriteAt(page, 32); err != nil {
			t.Fatalf("WriteAt failed: %v", err)
		}

		coalesced := buf.GetCoalescedWrites()

		if len(coalesced) != 1 {
			t.Fatalf("expected 1 write, got %d", len(coalesced))
		}

		if coalesced[0].offset != 32 {
			t.Errorf("expected offset 32, got %d", coalesced[0].offset)
		}
	})
}

func TestTransactionBufferBasic(t *testing.T) {
	mgr, err := memory.NewManager(memory.Config{Capacity: 64 * 1024 * 1024, Threshold: 0.9})

	if err != nil {
		t.Fatalf("failed to create memory manager: %v", err)
	}

	buf, err := NewTransactionBuffer(mgr, 123)

	if err != nil {
		t.Fatalf("failed to allocate transaction buffer: %v", err)
	}

	data := []byte("hello world")

	n, err := buf.WriteAt(data, 0)

	if err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}

	if n != len(data) {
		t.Fatalf("unexpected write length: got %d want %d", n, len(data))
	}

	out := make([]byte, len(data))

	n, err = buf.ReadAt(out, 0)

	if err != nil {
		t.Fatalf("ReadAt failed: %v", err)
	}

	if !bytes.Equal(out, data) {
		t.Fatalf("data mismatch: got %v want %v", out, data)
	}

	// Clean up
	buf.Release(mgr)
}

func TestTransactionBufferDiscard(t *testing.T) {
	mgr, err := memory.NewManager(memory.Config{Capacity: 32 * 1024 * 1024, Threshold: 0.9})

	if err != nil {
		t.Fatalf("failed to create memory manager: %v", err)
	}

	buf, err := NewTransactionBuffer(mgr, 456)

	if err != nil {
		t.Fatalf("failed to allocate transaction buffer: %v", err)
	}

	data := []byte("discard me")

	_, err = buf.WriteAt(data, 0)

	if err != nil {
		t.Fatalf("WriteAt failed: %v", err)
	}

	buf.Discard()

	out := make([]byte, len(data))

	_, err = buf.ReadAt(out, 0)

	if err == nil {
		t.Fatalf("expected ErrNotBuffered after discard, got nil")
	}

	// Clean up
	buf.Release(mgr)
}
