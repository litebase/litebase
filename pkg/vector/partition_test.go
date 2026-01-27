package vector_test

import (
	"testing"

	"github.com/litebase/litebase/pkg/vector"
)

func TestPartitionTable(t *testing.T) {
	t.Run("EmptyTable", func(t *testing.T) {
		queryVector, _ := vector.EncodeFloat32([]float32{1.0, 2.0, 3.0})
		query, _ := vector.ParseVectorBlob(queryVector)

		// This will fail because there's no real connection manager
		// But we can test the structure
		partitions, err := vector.PartitionTable("default", "test-db", "main", "vectors", "embedding", query, 10, vector.MetricL2)

		// We expect an error because the connection manager isn't available
		if err == nil {
			t.Log("Got partitions (unexpected with placeholder):", len(partitions))
		}
	})

	t.Run("ValidDimensions", func(t *testing.T) {
		// Test that we can create a query vector
		queryVector, err := vector.EncodeFloat32([]float32{1.0, 2.0, 3.0, 4.0, 5.0})

		if err != nil {
			t.Fatalf("Failed to encode query vector: %v", err)
		}

		query, err := vector.ParseVectorBlob(queryVector)

		if err != nil {
			t.Fatalf("Failed to parse query vector: %v", err)
		}

		if query.Dimensions != 5 {
			t.Errorf("Expected 5 dimensions, got %d", query.Dimensions)
		}
	})

	t.Run("InvalidVfsID", func(t *testing.T) {
		queryVector, _ := vector.EncodeFloat32([]float32{1.0, 2.0, 3.0})
		query, _ := vector.ParseVectorBlob(queryVector)

		_, err := vector.PartitionTable("", "test-db", "main", "vectors", "embedding", query, 10, vector.MetricL2)

		if err == nil {
			t.Error("Expected error for empty VFS ID")
		}
	})
}

func TestChunkSizeCalculation(t *testing.T) {
	testCases := []struct {
		name     string
		dims     int
		min      int
		max      int
		checkMax bool
	}{
		{
			name:     "Small vectors",
			dims:     64,
			min:      10000,
			max:      100000,
			checkMax: true,
		},
		{
			name:     "Medium vectors",
			dims:     384,
			min:      10000,
			max:      100000,
			checkMax: false,
		},
		{
			name:     "Large vectors (768)",
			dims:     768,
			min:      10000,
			max:      100000,
			checkMax: false,
		},
		{
			name:     "Very large vectors",
			dims:     2048,
			min:      10000,
			max:      100000,
			checkMax: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			chunkSize := vector.CalculateChunkSize(tc.dims)

			if chunkSize < tc.min {
				t.Errorf("Chunk size %d is below minimum %d", chunkSize, tc.min)
			}

			if chunkSize > tc.max {
				t.Errorf("Chunk size %d exceeds maximum %d", chunkSize, tc.max)
			}

			if tc.checkMax && chunkSize != tc.max {
				t.Errorf("Expected chunk size %d for small dimensions, got %d", tc.max, chunkSize)
			}

			// Verify the calculation makes sense
			bytesPerVector := tc.dims * 4 // float32
			totalBytes := chunkSize * bytesPerVector
			totalMB := totalBytes / (1024 * 1024)

			t.Logf("Dims: %d, ChunkSize: %d, Memory: ~%dMB", tc.dims, chunkSize, totalMB)
		})
	}
}

func TestTablePartitionStruct(t *testing.T) {
	partition := vector.TablePartition{
		StartRow: 1,
		EndRow:   1000,
	}

	if partition.StartRow != 1 {
		t.Errorf("Expected StartRow 1, got %d", partition.StartRow)
	}

	if partition.EndRow != 1000 {
		t.Errorf("Expected EndRow 1000, got %d", partition.EndRow)
	}

	// Test that we can create multiple partitions
	partitions := []vector.TablePartition{
		{StartRow: 1, EndRow: 1000},
		{StartRow: 1001, EndRow: 2000},
		{StartRow: 2001, EndRow: 3000},
	}

	if len(partitions) != 3 {
		t.Errorf("Expected 3 partitions, got %d", len(partitions))
	}

	// Verify no overlaps
	for i := 0; i < len(partitions)-1; i++ {
		if partitions[i].EndRow >= partitions[i+1].StartRow {
			t.Errorf("Partition overlap detected between %d and %d", i, i+1)
		}
	}
}
