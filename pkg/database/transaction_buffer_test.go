package database

import (
	"bytes"
	"testing"

	"github.com/litebase/litebase/pkg/memory"
)

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
