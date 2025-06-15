package storage_test

import (
	"encoding/binary"
	"testing"

	"github.com/litebase/litebase/pkg/storage"
)

func TestNewPageLogIndexEntry(t *testing.T) {
	entry := storage.NewPageLogIndexEntry(
		storage.PageNumber(1),
		storage.PageVersion(1),
		100,
		false,
	)

	if entry.PageNumber != storage.PageNumber(1) {
		t.Errorf("Expected PageNumber to be 1, got %d", entry.PageNumber)
	}

	if entry.Version != storage.PageVersion(1) {
		t.Errorf("Expected Version to be 1, got %d", entry.Version)
	}

	if entry.Offset != 100 {
		t.Errorf("Expected Offset to be 100, got %d", entry.Offset)
	}

	if entry.Tombstoned {
		t.Error("Expected Tombstoned to be false")
	}
}

func TestEncodePageLogIndexEntry(t *testing.T) {
	entry := storage.NewPageLogIndexEntry(
		storage.PageNumber(1),
		storage.PageVersion(1),
		100,
		false,
	)

	encoded := entry.Encode()

	if len(encoded) != storage.PageLogIndexEntryLength {
		t.Errorf("Expected encoded length to be %d, got %d", storage.PageLogIndexEntryLength, len(encoded))
	}

	if binary.LittleEndian.Uint64(encoded[0:8]) != uint64(entry.PageNumber) {
		t.Errorf("Expected PageNumber to be encoded as 1, got %d", binary.LittleEndian.Uint64(encoded[0:8]))
	}

	if binary.LittleEndian.Uint64(encoded[8:16]) != uint64(entry.Version) {
		t.Errorf("Expected Version to be encoded as 1, got %d", binary.LittleEndian.Uint64(encoded[8:16]))
	}

	if binary.LittleEndian.Uint64(encoded[16:24]) != uint64(100) {
		t.Errorf("Expected Offset to be encoded as 100, got %d", binary.LittleEndian.Uint64(encoded[16:24]))
	}

	if encoded[24] != 0 {
		t.Error("Expected tombstoned byte to be 0 for non-tombstoned entry")
	}
}
func TestDecodePageLogIndexEntry(t *testing.T) {
	encoded := make([]byte, storage.PageLogIndexEntryLength)
	encoded[0] = 1
	encoded[8] = 1
	encoded[16] = 100
	encoded[24] = 0

	entry := storage.DecodePageLogIndexEntry(encoded)

	if entry.PageNumber != storage.PageNumber(1) {
		t.Errorf("Expected PageNumber to be 1, got %d", entry.PageNumber)
	}

	if entry.Version != storage.PageVersion(1) {
		t.Errorf("Expected Version to be 1, got %d", entry.Version)
	}

	if entry.Offset != 100 {
		t.Errorf("Expected Offset to be 100, got %d", entry.Offset)
	}

	if entry.Tombstoned {
		t.Error("Expected Tombstoned to be false")
	}
}
