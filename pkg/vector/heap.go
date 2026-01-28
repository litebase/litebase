package vector

import (
	"container/heap"
	"sort"
)

// VectorResult represents a single k-NN search result
type VectorResult struct {
	Rowid    int64
	Distance float64
}

// TopKHeap is a min-heap that maintains the top k results
type TopKHeap struct {
	k       int
	results []VectorResult
}

// NewTopKHeap creates a new TopKHeap
func NewTopKHeap(k int) *TopKHeap {
	return &TopKHeap{
		k:       k,
		results: make([]VectorResult, 0, k),
	}
}

// Len implements heap.Interface
func (h *TopKHeap) Len() int {
	return len(h.results)
}

// Less implements heap.Interface (max heap - we want to remove largest distances)
func (h *TopKHeap) Less(i, j int) bool {
	if h.results[i].Distance == h.results[j].Distance {
		return h.results[i].Rowid > h.results[j].Rowid
	}

	return h.results[i].Distance > h.results[j].Distance
}

// Swap implements heap.Interface
func (h *TopKHeap) Swap(i, j int) {
	h.results[i], h.results[j] = h.results[j], h.results[i]
}

// Push implements heap.Interface
func (h *TopKHeap) Push(x interface{}) {
	h.results = append(h.results, x.(VectorResult))
}

// Pop implements heap.Interface
func (h *TopKHeap) Pop() interface{} {
	old := h.results
	n := len(old)
	x := old[n-1]
	h.results = old[0 : n-1]

	return x
}

// Insert adds a result to the heap, maintaining only top k
func (h *TopKHeap) Insert(rowid int64, distance float64) {
	result := VectorResult{Rowid: rowid, Distance: distance}

	if len(h.results) < h.k {
		heap.Push(h, result)
		return
	}

	if distance < h.results[0].Distance || (distance == h.results[0].Distance && rowid < h.results[0].Rowid) {
		heap.Pop(h)
		heap.Push(h, result)
	}
}

// Results returns the results sorted by distance ascending
// Pre-allocates with exact capacity to minimize allocations
func (h *TopKHeap) Results() []VectorResult {
	// Pre-allocate with exact length (not capacity) to avoid slice growth
	sorted := make([]VectorResult, len(h.results))
	copy(sorted, h.results)

	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].Distance == sorted[j].Distance {
			return sorted[i].Rowid < sorted[j].Rowid
		}

		return sorted[i].Distance < sorted[j].Distance
	})

	return sorted
}

// MergeHeaps merges multiple heaps into a single sorted result list
// Pre-allocates results slice to avoid allocations during merge (Phase 1 optimization)
func MergeHeaps(heaps []*TopKHeap, k int) []VectorResult {
	finalHeap := NewTopKHeap(k)

	for _, h := range heaps {
		for _, result := range h.results {
			finalHeap.Insert(result.Rowid, result.Distance)
		}
	}

	return finalHeap.Results()
}

// MergeWith merges another heap into this heap
// Phase 2.5: Enables continuous merging during streaming
func (h *TopKHeap) MergeWith(other *TopKHeap) {
	if other == nil {
		return
	}

	for _, result := range other.results {
		h.Insert(result.Rowid, result.Distance)
	}
}

// CalculateChunkSize determines optimal chunk size based on vector dimensions
func CalculateChunkSize(dimensions int) int {
	const minChunkSize = 25000
	const maxChunkSize = 250000
	const targetMemoryMB = 400

	bytesPerVector := dimensions * 4
	targetBytes := targetMemoryMB * 1024 * 1024
	chunkSize := targetBytes / bytesPerVector

	if chunkSize < minChunkSize {
		chunkSize = minChunkSize
	}

	if chunkSize > maxChunkSize {
		chunkSize = maxChunkSize
	}

	return chunkSize
}
