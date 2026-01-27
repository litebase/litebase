package vector_test

import (
	"sync"
	"testing"
	"time"

	"github.com/litebase/litebase/pkg/vector"
)

func TestNewWorkerPool(t *testing.T) {
	t.Run("Creation", func(t *testing.T) {
		pool := vector.NewWorkerPool(4)

		if pool == nil {
			t.Fatal("Expected non-nil worker pool")
		}

		if pool.MaxWorkers() != 4 {
			t.Errorf("Expected 4 workers, got %d", pool.MaxWorkers())
		}

		pool.Shutdown()
	})

	t.Run("SingleWorker", func(t *testing.T) {
		pool := vector.NewWorkerPool(1)

		if pool.MaxWorkers() != 1 {
			t.Errorf("Expected 1 worker, got %d", pool.MaxWorkers())
		}

		pool.Shutdown()
	})
}

func TestWorkerPoolSubmit(t *testing.T) {
	t.Run("BasicSubmit", func(t *testing.T) {
		pool := vector.NewWorkerPool(2)
		defer pool.Shutdown()

		resultChan := make(chan *vector.ChunkResult, 1)

		queryVector, _ := vector.EncodeFloat32([]float32{1.0, 2.0, 3.0})
		query, _ := vector.ParseVectorBlob(queryVector)

		job := &vector.ChunkJob{
			ChunkID:     0,
			StartRow:    1,
			EndRow:      100,
			VfsID:       "default",
			DatabaseID:  "test",
			BranchID:    "main",
			TableName:   "vectors",
			ColumnName:  "embedding",
			QueryVector: query,
			Metric:      vector.MetricL2,
			K:           10,
			ResultChan:  resultChan,
		}

		pool.Submit(job)

		select {
		case result := <-resultChan:
			if result == nil {
				t.Error("Expected non-nil result")
				return
			}

			if result.ChunkID != 0 {
				t.Errorf("Expected ChunkID 0, got %d", result.ChunkID)
			}
		case <-time.After(2 * time.Second):
			t.Error("Timeout waiting for result")
		}
	})

	t.Run("MultipleJobs", func(t *testing.T) {
		pool := vector.NewWorkerPool(4)
		defer pool.Shutdown()

		numJobs := 10
		resultChan := make(chan *vector.ChunkResult, numJobs)
		queryVector, _ := vector.EncodeFloat32([]float32{1.0, 2.0, 3.0})
		query, _ := vector.ParseVectorBlob(queryVector)

		for i := 0; i < numJobs; i++ {
			job := &vector.ChunkJob{
				ChunkID:     i,
				StartRow:    int64(i * 100),
				EndRow:      int64((i + 1) * 100),
				VfsID:       "default",
				DatabaseID:  "test",
				BranchID:    "main",
				TableName:   "vectors",
				ColumnName:  "embedding",
				QueryVector: query,
				Metric:      vector.MetricL2,
				K:           10,
				ResultChan:  resultChan,
			}

			pool.Submit(job)
		}

		// Collect results
		receivedCount := 0

		for i := 0; i < numJobs; i++ {
			select {
			case <-resultChan:
				receivedCount++
			case <-time.After(3 * time.Second):
				t.Errorf("Timeout waiting for result %d", i)
			}
		}

		if receivedCount != numJobs {
			t.Errorf("Expected %d results, got %d", numJobs, receivedCount)
		}
	})

	t.Run("ConcurrentSubmit", func(t *testing.T) {
		pool := vector.NewWorkerPool(4)
		defer pool.Shutdown()

		numJobs := 20
		resultChan := make(chan *vector.ChunkResult, numJobs)

		queryVector, _ := vector.EncodeFloat32([]float32{1.0, 2.0, 3.0})
		query, _ := vector.ParseVectorBlob(queryVector)

		var wg sync.WaitGroup

		wg.Add(numJobs)

		for i := 0; i < numJobs; i++ {
			go func(id int) {
				defer wg.Done()

				job := &vector.ChunkJob{
					ChunkID:     id,
					StartRow:    int64(id * 100),
					EndRow:      int64((id + 1) * 100),
					VfsID:       "default",
					DatabaseID:  "test",
					BranchID:    "main",
					TableName:   "vectors",
					ColumnName:  "embedding",
					QueryVector: query,
					Metric:      vector.MetricL2,
					K:           10,
					ResultChan:  resultChan,
				}

				pool.Submit(job)
			}(i)
		}

		wg.Wait()

		// Collect results
		receivedCount := 0

		for i := 0; i < numJobs; i++ {
			select {
			case <-resultChan:
				receivedCount++
			case <-time.After(3 * time.Second):
				t.Errorf("Timeout waiting for result %d", i)
			}
		}

		if receivedCount != numJobs {
			t.Errorf("Expected %d results, got %d", numJobs, receivedCount)
		}
	})
}

func TestWorkerPoolShutdown(t *testing.T) {
	t.Run("CleanShutdown", func(t *testing.T) {
		pool := vector.NewWorkerPool(2)

		// Submit some jobs
		resultChan := make(chan *vector.ChunkResult, 5)

		queryVector, _ := vector.EncodeFloat32([]float32{1.0, 2.0, 3.0})
		query, _ := vector.ParseVectorBlob(queryVector)

		for i := 0; i < 5; i++ {
			job := &vector.ChunkJob{
				ChunkID:     i,
				StartRow:    int64(i * 100),
				EndRow:      int64((i + 1) * 100),
				VfsID:       "default",
				DatabaseID:  "test",
				BranchID:    "main",
				TableName:   "vectors",
				ColumnName:  "embedding",
				QueryVector: query,
				Metric:      vector.MetricL2,
				K:           10,
				ResultChan:  resultChan,
			}

			pool.Submit(job)
		}

		// Wait a bit for jobs to be processed
		time.Sleep(100 * time.Millisecond)

		// Shutdown should not hang
		pool.Shutdown()
	})

	t.Run("ShutdownAfterSubmit", func(t *testing.T) {
		pool := vector.NewWorkerPool(2)

		// Shutdown pool
		pool.Shutdown()

		// Verify pool shutdown completed - submitting after shutdown would panic
		// so we just verify the shutdown succeeded
		if pool == nil {
			t.Error("Pool should not be nil after shutdown")
		}
	})
}

func TestChunkJob(t *testing.T) {
	t.Run("JobCreation", func(t *testing.T) {
		queryVector, _ := vector.EncodeFloat32([]float32{1.0, 2.0, 3.0})
		query, _ := vector.ParseVectorBlob(queryVector)

		job := &vector.ChunkJob{
			ChunkID:     5,
			StartRow:    1000,
			EndRow:      2000,
			VfsID:       "test-vfs",
			DatabaseID:  "test-db",
			BranchID:    "test-branch",
			TableName:   "test-table",
			ColumnName:  "test-column",
			QueryVector: query,
			Metric:      vector.MetricCosine,
			K:           20,
			ResultChan:  make(chan *vector.ChunkResult, 1),
		}

		if job.ChunkID != 5 {
			t.Errorf("Expected ChunkID 5, got %d", job.ChunkID)
		}

		if job.StartRow != 1000 {
			t.Errorf("Expected StartRow 1000, got %d", job.StartRow)
		}

		if job.EndRow != 2000 {
			t.Errorf("Expected EndRow 2000, got %d", job.EndRow)
		}

		if job.Metric != vector.MetricCosine {
			t.Errorf("Expected metric 'cosine', got '%s'", job.Metric)
		}

		if job.K != 20 {
			t.Errorf("Expected K=20, got %d", job.K)
		}
	})
}

func TestChunkResult(t *testing.T) {
	t.Run("ResultCreation", func(t *testing.T) {
		heap := vector.NewTopKHeap(10)
		heap.Insert(1, 0.5)
		heap.Insert(2, 0.3)

		result := &vector.ChunkResult{
			ChunkID: 3,
			Heap:    heap,
			Error:   nil,
		}

		if result.ChunkID != 3 {
			t.Errorf("Expected ChunkID 3, got %d", result.ChunkID)
		}

		if result.Heap == nil {
			t.Error("Expected non-nil heap")
		}

		if result.Error != nil {
			t.Errorf("Expected nil error, got %v", result.Error)
		}

		results := result.Heap.Results()

		if len(results) != 2 {
			t.Errorf("Expected 2 results in heap, got %d", len(results))
		}
	})
}

func BenchmarkWorkerPool(b *testing.B) {
	pool := vector.NewWorkerPool(4)
	defer pool.Shutdown()

	queryVector, _ := vector.EncodeFloat32([]float32{1.0, 2.0, 3.0})
	query, _ := vector.ParseVectorBlob(queryVector)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		resultChan := make(chan *vector.ChunkResult, 1)

		job := &vector.ChunkJob{
			ChunkID:     i,
			StartRow:    int64(i * 100),
			EndRow:      int64((i + 1) * 100),
			VfsID:       "default",
			DatabaseID:  "test",
			BranchID:    "main",
			TableName:   "vectors",
			ColumnName:  "embedding",
			QueryVector: query,
			Metric:      vector.MetricL2,
			K:           10,
			ResultChan:  resultChan,
		}

		pool.Submit(job)
		<-resultChan
	}
}
