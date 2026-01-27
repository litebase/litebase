package vector_test

import (
	"testing"

	"github.com/litebase/litebase/pkg/vector"
)

func TestMinHeap(t *testing.T) {
	t.Run("TopKHeap", func(t *testing.T) {
		heap := vector.NewTopKHeap(3)

		heap.Insert(1, 5.0)
		heap.Insert(2, 3.0)
		heap.Insert(3, 7.0)
		heap.Insert(4, 2.0)
		heap.Insert(5, 9.0)

		results := heap.Results()

		if len(results) != 3 {
			t.Errorf("Expected 3 results, got %d", len(results))
		}

		if results[0].Rowid != 4 || results[0].Distance != 2.0 {
			t.Errorf("First result incorrect: got rowid=%d dist=%f, want rowid=4 dist=2.0",
				results[0].Rowid, results[0].Distance)
		}

		if results[1].Rowid != 2 || results[1].Distance != 3.0 {
			t.Errorf("Second result incorrect: got rowid=%d dist=%f, want rowid=2 dist=3.0",
				results[1].Rowid, results[1].Distance)
		}

		if results[2].Rowid != 1 || results[2].Distance != 5.0 {
			t.Errorf("Third result incorrect: got rowid=%d dist=%f, want rowid=1 dist=5.0",
				results[2].Rowid, results[2].Distance)
		}
	})

	t.Run("TieBreaking", func(t *testing.T) {
		heap := vector.NewTopKHeap(2)

		heap.Insert(5, 1.0)
		heap.Insert(3, 1.0)
		heap.Insert(7, 1.0)

		results := heap.Results()

		if results[0].Rowid != 3 {
			t.Errorf("First result rowid incorrect: got %d, want 3", results[0].Rowid)
		}

		if results[1].Rowid != 5 {
			t.Errorf("Second result rowid incorrect: got %d, want 5", results[1].Rowid)
		}
	})
}

func TestChunkSize(t *testing.T) {
	testCases := []struct {
		dims     int
		expected int
	}{
		{64, 100000},  // Maxed out
		{768, 100000}, // Also hits max
		{1536, 68266}, // 400MB / (1536 * 4)
		{2048, 51200}, // 400MB / (2048 * 4)
	}

	for _, tc := range testCases {
		result := vector.CalculateChunkSize(tc.dims)

		if result != tc.expected {
			t.Errorf("Chunk size for %d dims: got %d, want %d", tc.dims, result, tc.expected)
		}
	}
}
