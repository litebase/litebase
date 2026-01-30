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

		if results[0].RowId != 4 || results[0].Distance != 2.0 {
			t.Errorf("First result incorrect: got rowid=%d dist=%f, want rowid=4 dist=2.0",
				results[0].RowId, results[0].Distance)
		}

		if results[1].RowId != 2 || results[1].Distance != 3.0 {
			t.Errorf("Second result incorrect: got rowid=%d dist=%f, want rowid=2 dist=3.0",
				results[1].RowId, results[1].Distance)
		}

		if results[2].RowId != 1 || results[2].Distance != 5.0 {
			t.Errorf("Third result incorrect: got rowid=%d dist=%f, want rowid=1 dist=5.0",
				results[2].RowId, results[2].Distance)
		}
	})

	t.Run("TieBreaking", func(t *testing.T) {
		heap := vector.NewTopKHeap(2)

		heap.Insert(5, 1.0)
		heap.Insert(3, 1.0)
		heap.Insert(7, 1.0)

		results := heap.Results()

		if results[0].RowId != 3 {
			t.Errorf("First result rowid incorrect: got %d, want 3", results[0].RowId)
		}

		if results[1].RowId != 5 {
			t.Errorf("Second result rowid incorrect: got %d, want 5", results[1].RowId)
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

func TestMergeHeaps(t *testing.T) {
	t.Run("MergeMultipleHeaps", func(t *testing.T) {
		heap1 := vector.NewTopKHeap(3)
		heap1.Insert(1, 5.0)
		heap1.Insert(2, 3.0)
		heap1.Insert(3, 7.0)

		heap2 := vector.NewTopKHeap(3)
		heap2.Insert(4, 2.0)
		heap2.Insert(5, 9.0)
		heap2.Insert(6, 1.0)

		heap3 := vector.NewTopKHeap(3)
		heap3.Insert(7, 4.0)
		heap3.Insert(8, 6.0)
		heap3.Insert(9, 8.0)

		heaps := []*vector.TopKHeap{heap1, heap2, heap3}
		results := vector.MergeHeaps(heaps, 5)

		if len(results) != 5 {
			t.Errorf("Expected 5 results, got %d", len(results))
		}

		expected := []struct {
			rowid    int64
			distance float64
		}{
			{6, 1.0},
			{4, 2.0},
			{2, 3.0},
			{7, 4.0},
			{1, 5.0},
		}

		for i, exp := range expected {
			if results[i].RowId != exp.rowid || results[i].Distance != exp.distance {
				t.Errorf("Result %d: got rowid=%d dist=%f, want rowid=%d dist=%f",
					i, results[i].RowId, results[i].Distance, exp.rowid, exp.distance)
			}
		}
	})

	t.Run("MergeEmptyHeaps", func(t *testing.T) {
		heap1 := vector.NewTopKHeap(3)
		heap2 := vector.NewTopKHeap(3)

		heaps := []*vector.TopKHeap{heap1, heap2}
		results := vector.MergeHeaps(heaps, 5)

		if len(results) != 0 {
			t.Errorf("Expected 0 results from empty heaps, got %d", len(results))
		}
	})

	t.Run("MergeSingleHeap", func(t *testing.T) {
		heap1 := vector.NewTopKHeap(3)
		heap1.Insert(1, 5.0)
		heap1.Insert(2, 3.0)

		heaps := []*vector.TopKHeap{heap1}
		results := vector.MergeHeaps(heaps, 3)

		if len(results) != 2 {
			t.Errorf("Expected 2 results, got %d", len(results))
		}

		if results[0].RowId != 2 || results[0].Distance != 3.0 {
			t.Errorf("First result incorrect: got rowid=%d dist=%f, want rowid=2 dist=3.0",
				results[0].RowId, results[0].Distance)
		}
	})

	t.Run("MergeWithTies", func(t *testing.T) {
		heap1 := vector.NewTopKHeap(2)
		heap1.Insert(5, 1.0)
		heap1.Insert(3, 1.0)

		heap2 := vector.NewTopKHeap(2)
		heap2.Insert(7, 1.0)
		heap2.Insert(2, 1.0)

		heaps := []*vector.TopKHeap{heap1, heap2}
		results := vector.MergeHeaps(heaps, 3)

		if len(results) != 3 {
			t.Errorf("Expected 3 results, got %d", len(results))
		}

		if results[0].Distance != 1.0 || results[1].Distance != 1.0 || results[2].Distance != 1.0 {
			t.Errorf("All distances should be 1.0")
		}

		if results[0].RowId >= results[1].RowId || results[1].RowId >= results[2].RowId {
			t.Errorf("Results with same distance should be sorted by RowId ascending")
		}
	})
}

func TestMergeWith(t *testing.T) {
	t.Run("MergeNonEmptyHeaps", func(t *testing.T) {
		heap1 := vector.NewTopKHeap(3)
		heap1.Insert(1, 5.0)
		heap1.Insert(2, 3.0)

		heap2 := vector.NewTopKHeap(3)
		heap2.Insert(4, 2.0)
		heap2.Insert(5, 7.0)

		heap1.MergeWith(heap2)

		results := heap1.Results()

		if len(results) != 3 {
			t.Errorf("Expected 3 results after merge, got %d", len(results))
		}

		expected := []struct {
			rowid    int64
			distance float64
		}{
			{4, 2.0},
			{2, 3.0},
			{1, 5.0},
		}

		for i, exp := range expected {
			if results[i].RowId != exp.rowid || results[i].Distance != exp.distance {
				t.Errorf("Result %d: got rowid=%d dist=%f, want rowid=%d dist=%f",
					i, results[i].RowId, results[i].Distance, exp.rowid, exp.distance)
			}
		}
	})

	t.Run("MergeWithNilHeap", func(t *testing.T) {
		heap1 := vector.NewTopKHeap(3)
		heap1.Insert(1, 5.0)
		heap1.Insert(2, 3.0)

		originalLen := heap1.Len()

		heap1.MergeWith(nil)

		if heap1.Len() != originalLen {
			t.Errorf("Merging with nil should not change heap size: got %d, want %d",
				heap1.Len(), originalLen)
		}
	})

	t.Run("MergeWithEmptyHeap", func(t *testing.T) {
		heap1 := vector.NewTopKHeap(3)
		heap1.Insert(1, 5.0)
		heap1.Insert(2, 3.0)

		heap2 := vector.NewTopKHeap(3)

		originalLen := heap1.Len()

		heap1.MergeWith(heap2)

		if heap1.Len() != originalLen {
			t.Errorf("Merging with empty heap should not change heap size: got %d, want %d",
				heap1.Len(), originalLen)
		}
	})

	t.Run("MergeIntoEmptyHeap", func(t *testing.T) {
		heap1 := vector.NewTopKHeap(3)

		heap2 := vector.NewTopKHeap(3)
		heap2.Insert(4, 2.0)
		heap2.Insert(5, 7.0)

		heap1.MergeWith(heap2)

		results := heap1.Results()

		if len(results) != 2 {
			t.Errorf("Expected 2 results after merge, got %d", len(results))
		}

		if results[0].RowId != 4 || results[0].Distance != 2.0 {
			t.Errorf("First result incorrect: got rowid=%d dist=%f, want rowid=4 dist=2.0",
				results[0].RowId, results[0].Distance)
		}
	})

	t.Run("MergeRespectKLimit", func(t *testing.T) {
		heap1 := vector.NewTopKHeap(2)
		heap1.Insert(1, 5.0)
		heap1.Insert(2, 3.0)

		heap2 := vector.NewTopKHeap(2)
		heap2.Insert(4, 2.0)
		heap2.Insert(5, 7.0)
		heap2.Insert(6, 1.0)

		heap1.MergeWith(heap2)

		results := heap1.Results()

		if len(results) != 2 {
			t.Errorf("Expected 2 results (k=2), got %d", len(results))
		}

		if results[0].RowId != 6 || results[0].Distance != 1.0 {
			t.Errorf("First result should be best: got rowid=%d dist=%f, want rowid=6 dist=1.0",
				results[0].RowId, results[0].Distance)
		}

		if results[1].RowId != 4 || results[1].Distance != 2.0 {
			t.Errorf("Second result incorrect: got rowid=%d dist=%f, want rowid=4 dist=2.0",
				results[1].RowId, results[1].Distance)
		}
	})
}
