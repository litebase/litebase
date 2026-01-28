package vector

import (
	"testing"
)

// Benchmark blob parsing
func BenchmarkParseVectorBlob(b *testing.B) {
	dims := 128
	data := make([]float32, dims)
	for i := range data {
		data[i] = float32(i) * 0.1
	}

	blob, _ := EncodeFloat32(data)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := ParseVectorBlob(blob)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// Benchmark pooled blob parsing
func BenchmarkParseVectorBlobPooled(b *testing.B) {
	dims := 128
	data := make([]float32, dims)
	for i := range data {
		data[i] = float32(i) * 0.1
	}

	blob, _ := EncodeFloat32(data)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		vb, err := ParseVectorBlobPooled(blob)
		if err != nil {
			b.Fatal(err)
		}
		PutVectorBlob(vb)
	}
}

// Benchmark L2 distance calculation
func BenchmarkDistanceL2(b *testing.B) {
	dims := 128
	vec1 := make([]float32, dims)
	vec2 := make([]float32, dims)

	for i := range vec1 {
		vec1[i] = float32(i) * 0.1
		vec2[i] = float32(i) * 0.2
	}

	blob1, _ := EncodeFloat32(vec1)
	blob2, _ := EncodeFloat32(vec2)

	vb1, _ := ParseVectorBlob(blob1)
	vb2, _ := ParseVectorBlob(blob2)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := DistanceL2(vb1, vb2)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// Benchmark cosine distance calculation
func BenchmarkDistanceCosine(b *testing.B) {
	dims := 128
	vec1 := make([]float32, dims)
	vec2 := make([]float32, dims)

	for i := range vec1 {
		vec1[i] = float32(i) * 0.1
		vec2[i] = float32(i) * 0.2
	}

	blob1, _ := EncodeFloat32(vec1)
	blob2, _ := EncodeFloat32(vec2)

	vb1, _ := ParseVectorBlob(blob1)
	vb2, _ := ParseVectorBlob(blob2)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := DistanceCosine(vb1, vb2)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// Benchmark dot product distance
func BenchmarkDistanceDot(b *testing.B) {
	dims := 128
	vec1 := make([]float32, dims)
	vec2 := make([]float32, dims)

	for i := range vec1 {
		vec1[i] = float32(i) * 0.1
		vec2[i] = float32(i) * 0.2
	}

	blob1, _ := EncodeFloat32(vec1)
	blob2, _ := EncodeFloat32(vec2)

	vb1, _ := ParseVectorBlob(blob1)
	vb2, _ := ParseVectorBlob(blob2)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := DistanceDot(vb1, vb2)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// Benchmark parse + distance combined
func BenchmarkParseAndDistance(b *testing.B) {
	dims := 128
	vec1 := make([]float32, dims)
	vec2 := make([]float32, dims)

	for i := range vec1 {
		vec1[i] = float32(i) * 0.1
		vec2[i] = float32(i) * 0.2
	}

	blob1, _ := EncodeFloat32(vec1)
	blob2, _ := EncodeFloat32(vec2)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		vb1, _ := ParseVectorBlob(blob1)
		vb2, _ := ParseVectorBlob(blob2)
		_, err := DistanceL2(vb1, vb2)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// Benchmark TopKHeap operations
func BenchmarkTopKHeapInsert(b *testing.B) {
	k := 10
	heap := NewTopKHeap(k)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		heap.Insert(int64(i), float64(i%100))
	}
}

// Benchmark merging heaps
func BenchmarkMergeHeaps(b *testing.B) {
	k := 10
	numHeaps := 40 // Simulating 40 workers (2x20 CPUs)

	heaps := make([]*TopKHeap, numHeaps)
	for i := range heaps {
		heaps[i] = NewTopKHeap(k)
		for j := 0; j < 100; j++ {
			heaps[i].Insert(int64(i*100+j), float64(j))
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = MergeHeaps(heaps, k)
	}
}
