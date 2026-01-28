package vector

import (
	"fmt"
	"testing"
)

// BenchmarkStreamingHeapMerge benchmarks the streaming heap merge
// Simulates continuous merging of batch heaps as they arrive
func BenchmarkStreamingHeapMerge(b *testing.B) {
	const k = 10
	const numBatches = 10 // Simulates 10 batches of 2500 rows each

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Central heap that continuously merges incoming batches
		centralHeap := NewTopKHeap(k)

		// Simulate streaming batches
		for batchID := 0; batchID < numBatches; batchID++ {
			// Create a batch heap (simulating results from a 2500-row batch)
			batchHeap := NewTopKHeap(k)

			for j := 0; j < k; j++ {
				rowid := int64(batchID*k + j)
				distance := float64(j) * 0.1
				batchHeap.Insert(rowid, distance)
			}

			// Merge batch into central heap (Phase 2.5 streaming merge)
			centralHeap.MergeWith(batchHeap)
		}

		// Get final results
		_ = centralHeap.Results()
	}
}

// BenchmarkBatchVsStreaming compares old scatter-gather vs new streaming
func BenchmarkBatchVsStreaming(b *testing.B) {
	const k = 10
	const numHeaps = 10

	b.Run("ScatterGather", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			// Old way: collect all heaps then merge at end
			heaps := make([]*TopKHeap, numHeaps)

			for j := 0; j < numHeaps; j++ {
				heap := NewTopKHeap(k)

				for r := 0; r < k; r++ {
					heap.Insert(int64(j*k+r), float64(r)*0.1)
				}

				heaps[j] = heap
			}

			// Merge all at once
			_ = MergeHeaps(heaps, k)
		}
	})

	b.Run("Streaming", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			// New way: merge continuously as heaps arrive
			centralHeap := NewTopKHeap(k)

			for j := 0; j < numHeaps; j++ {
				batchHeap := NewTopKHeap(k)

				for r := 0; r < k; r++ {
					batchHeap.Insert(int64(j*k+r), float64(r)*0.1)
				}

				// Merge immediately instead of waiting
				centralHeap.MergeWith(batchHeap)
			}

			_ = centralHeap.Results()
		}
	})
}

// TestStreamingArchitecture validates the Phase 2.5 streaming design
func TestStreamingArchitecture(t *testing.T) {
	const k = 10
	centralHeap := NewTopKHeap(k)

	// Simulate 5 batches streaming in
	for batchID := 0; batchID < 5; batchID++ {
		batchHeap := NewTopKHeap(k)

		// Each batch has some results
		for i := 0; i < 5; i++ {
			rowid := int64(batchID*100 + i)
			distance := float64(batchID + i)
			batchHeap.Insert(rowid, distance)
		}

		// Merge batch into central heap
		centralHeap.MergeWith(batchHeap)
	}

	// Get final results
	results := centralHeap.Results()

	if len(results) == 0 {
		t.Fatal("Expected results from streaming merge")
	}

	// Verify results are sorted
	for i := 1; i < len(results); i++ {
		if results[i].Distance < results[i-1].Distance {
			t.Errorf("Results not sorted: result[%d]=%f < result[%d]=%f",
				i, results[i].Distance, i-1, results[i-1].Distance)
		}
	}

	t.Logf("Streaming merge produced %d results", len(results))
}

// BenchmarkBatchSizes compares different batch sizes
func BenchmarkBatchSizes(b *testing.B) {
	const k = 10
	batchSizes := []int{1000, 2500, 5000, 10000, 25000}

	for _, batchSize := range batchSizes {
		b.Run(fmt.Sprintf("Batch%d", batchSize), func(b *testing.B) {
			// Simulate total of 25000 rows across different batch sizes
			totalRows := 25000
			numBatches := totalRows / batchSize

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				centralHeap := NewTopKHeap(k)

				for batch := 0; batch < numBatches; batch++ {
					batchHeap := NewTopKHeap(k)

					// Process batch
					for row := 0; row < batchSize && row < k; row++ {
						batchHeap.Insert(int64(batch*batchSize+row), float64(row)*0.1)
					}

					centralHeap.MergeWith(batchHeap)
				}

				_ = centralHeap.Results()
			}
		})
	}
}
