package database_test

import (
	"encoding/binary"
	"testing"

	"github.com/litebase/litebase/server/database"
)

func TestCheckpointEncode(t *testing.T) {
	checkpoint := database.Checkpoint{
		BeginPageCount:    100,
		LargestPageNumber: 200,
		Offset:            300,
		Size:              400,
		Timestamp:         1234567890,
	}

	encoded := checkpoint.Encode()

	if len(encoded) != 48 {
		t.Errorf("Expected encoded length to be 48, got %d", len(encoded))
	}

	version := binary.LittleEndian.Uint64(encoded[0:8])

	if version != database.CheckpointVersion {
		t.Errorf("Expected version %d, got %d", database.CheckpointVersion, version)
	}

	beginPageCount := binary.LittleEndian.Uint64(encoded[8:16])

	if beginPageCount != uint64(checkpoint.BeginPageCount) {
		t.Errorf("Expected BeginPageCount %d, got %d", checkpoint.BeginPageCount, beginPageCount)
	}

	largestPageNumber := binary.LittleEndian.Uint64(encoded[16:24])

	if largestPageNumber != uint64(checkpoint.LargestPageNumber) {
		t.Errorf("Expected LargestPageNumber %d, got %d", checkpoint.LargestPageNumber, largestPageNumber)
	}

	offset := binary.LittleEndian.Uint64(encoded[24:32])

	if offset != uint64(checkpoint.Offset) {
		t.Errorf("Expected Offset %d, got %d", checkpoint.Offset, offset)
	}

	size := binary.LittleEndian.Uint64(encoded[32:40])

	if size != uint64(checkpoint.Size) {
		t.Errorf("Expected Size %d, got %d", checkpoint.Size, size)
	}

	timestamp := binary.LittleEndian.Uint64(encoded[40:48])

	if timestamp != uint64(checkpoint.Timestamp) {
		t.Errorf("Expected Timestamp %d, got %d", checkpoint.Timestamp, timestamp)
	}
}

func TestDecodeCheckpoint(t *testing.T) {
	original := database.Checkpoint{
		BeginPageCount:    100,
		LargestPageNumber: 200,
		Offset:            300,
		Size:              400,
		Timestamp:         1234567890,
	}

	encoded := original.Encode()
	decoded := database.DecodeCheckpoint(encoded)

	if decoded.BeginPageCount != original.BeginPageCount {
		t.Errorf("Expected BeginPageCount %d, got %d", original.BeginPageCount, decoded.BeginPageCount)
	}

	if decoded.LargestPageNumber != original.LargestPageNumber {
		t.Errorf("Expected LargestPageNumber %d, got %d", original.LargestPageNumber, decoded.LargestPageNumber)
	}

	if decoded.Offset != original.Offset {
		t.Errorf("Expected Offset %d, got %d", original.Offset, decoded.Offset)
	}

	if decoded.Size != original.Size {
		t.Errorf("Expected Size %d, got %d", original.Size, decoded.Size)
	}

	if decoded.Timestamp != original.Timestamp {
		t.Errorf("Expected Timestamp %d, got %d", original.Timestamp, decoded.Timestamp)
	}
}
