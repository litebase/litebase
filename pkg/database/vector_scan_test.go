package database_test

import (
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/database"
	"github.com/litebase/litebase/pkg/server"
	"github.com/litebase/litebase/pkg/sqlite3"
	"github.com/litebase/litebase/pkg/vector"
)

func TestVectorScanIntegration(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		conn, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

		if err != nil {
			t.Fatalf("Failed to get connection: %v", err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(conn)

		dbConn := conn.GetConnection()

		// Create embeddings table
		_, err = dbConn.Exec(`
			CREATE TABLE embeddings (
				id INTEGER PRIMARY KEY,
				vector BLOB
			)
		`, nil)

		if err != nil {
			t.Fatalf("Failed to create table: %v", err)
		}

		// Insert test vectors
		testVectors := [][]float32{
			{1.0, 0.0, 0.0, 0.0},
			{0.9, 0.1, 0.0, 0.0},
			{0.8, 0.2, 0.0, 0.0},
			{0.7, 0.3, 0.0, 0.0},
			{0.0, 1.0, 0.0, 0.0},
		}

		for i, vec := range testVectors {
			blob, err := vector.EncodeFloat32(vec)

			if err != nil {
				t.Fatalf("Failed to encode vector %d: %v", i, err)
			}

			_, err = dbConn.Exec(
				"INSERT INTO embeddings (id, vector) VALUES (?, ?)",
				[]sqlite3.StatementParameter{
					{Type: "INTEGER", Value: int64(i + 1)},
					{Type: "BLOB", Value: blob},
				},
			)

			if err != nil {
				t.Fatalf("Failed to insert vector %d: %v", i, err)
			}
		}

		vfsID := conn.GetConnection().VFSHash()

		// Test VectorScan with L2 metric
		t.Run("VectorScanL2", func(t *testing.T) {
			queryVector, err := vector.EncodeFloat32([]float32{1.0, 0.0, 0.0, 0.0})

			if err != nil {
				t.Fatalf("Failed to encode query vector: %v", err)
			}

			handle, err := database.VectorScan(
				vfsID,
				mock.DatabaseID,
				mock.DatabaseBranchID,
				"embeddings",
				"vector",
				queryVector,
				3,
				vector.MetricL2,
			)

			if err != nil {
				t.Fatalf("VectorScan failed: %v", err)
			}

			defer handle.Delete()

			// Retrieve results
			scanHandle := handle.Value().(*database.VectorScanHandle)

			if len(scanHandle.Results) != 3 {
				t.Errorf("Expected 3 results, got %d", len(scanHandle.Results))
			}

			// First result should be rowid 1 (exact match)
			if len(scanHandle.Results) > 0 && scanHandle.Results[0].RowId != 1 {
				t.Errorf("Expected first result rowid=1, got %d", scanHandle.Results[0].RowId)
			}

			// Verify results are sorted by distance
			for i := 1; i < len(scanHandle.Results); i++ {
				if scanHandle.Results[i].Distance < scanHandle.Results[i-1].Distance {
					t.Errorf("Results not sorted: result[%d].Distance=%f < result[%d].Distance=%f",
						i, scanHandle.Results[i].Distance, i-1, scanHandle.Results[i-1].Distance)
				}
			}
		})

		// Test VectorScan with Cosine metric
		t.Run("VectorScanCosine", func(t *testing.T) {
			queryVector, err := vector.EncodeFloat32([]float32{0.0, 1.0, 0.0, 0.0})

			if err != nil {
				t.Fatalf("Failed to encode query vector: %v", err)
			}

			handle, err := database.VectorScan(
				vfsID,
				mock.DatabaseID,
				mock.DatabaseBranchID,
				"embeddings",
				"vector",
				queryVector,
				3,
				vector.MetricCosine,
			)

			if err != nil {
				t.Fatalf("VectorScan failed: %v", err)
			}

			defer handle.Delete()

			scanHandle := handle.Value().(*database.VectorScanHandle)

			if len(scanHandle.Results) != 3 {
				t.Errorf("Expected 3 results, got %d", len(scanHandle.Results))
			}

			// First result should be rowid 5 (vector [0, 1, 0, 0])
			if len(scanHandle.Results) > 0 && scanHandle.Results[0].RowId != 5 {
				t.Errorf("Expected first result rowid=5, got %d", scanHandle.Results[0].RowId)
			}
		})

		// Test with k larger than table size
		t.Run("VectorScanLargeK", func(t *testing.T) {
			queryVector, err := vector.EncodeFloat32([]float32{1.0, 0.0, 0.0, 0.0})

			if err != nil {
				t.Fatalf("Failed to encode query vector: %v", err)
			}

			handle, err := database.VectorScan(
				vfsID,
				mock.DatabaseID,
				mock.DatabaseBranchID,
				"embeddings",
				"vector",
				queryVector,
				100,
				vector.MetricL2,
			)

			if err != nil {
				t.Fatalf("VectorScan failed: %v", err)
			}

			defer handle.Delete()

			scanHandle := handle.Value().(*database.VectorScanHandle)

			// Should return all 5 vectors
			if len(scanHandle.Results) != 5 {
				t.Errorf("Expected 5 results (all vectors), got %d", len(scanHandle.Results))
			}
		})
	})
}

func TestVectorScanParallelExecution(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		conn, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

		if err != nil {
			t.Fatalf("Failed to get connection: %v", err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(conn)

		dbConn := conn.GetConnection()

		// Create table with many vectors to trigger parallel execution
		_, err = dbConn.Exec(`
			CREATE TABLE large_embeddings (
				id INTEGER PRIMARY KEY,
				vector BLOB
			)
		`, nil)

		if err != nil {
			t.Fatalf("Failed to create table: %v", err)
		}

		// Insert enough vectors to trigger chunking (> 10000)
		numVectors := 15000
		dims := 4

		for i := 0; i < numVectors; i++ {
			vec := make([]float32, dims)

			for j := 0; j < dims; j++ {
				vec[j] = float32(i%10) / 10.0
			}

			blob, _ := vector.EncodeFloat32(vec)

			_, err = dbConn.Exec(
				"INSERT INTO large_embeddings (id, vector) VALUES (?, ?)",
				[]sqlite3.StatementParameter{
					{Type: "INTEGER", Value: int64(i + 1)},
					{Type: "BLOB", Value: blob},
				},
			)

			if err != nil {
				t.Fatalf("Failed to insert vector %d: %v", i, err)
			}
		}

		vfsID := conn.GetConnection().VFSHash()

		t.Run("ParallelScan", func(t *testing.T) {
			queryVector, err := vector.EncodeFloat32([]float32{0.5, 0.5, 0.5, 0.5})

			if err != nil {
				t.Fatalf("Failed to encode query vector: %v", err)
			}

			start := time.Now()

			handle, err := database.VectorScan(
				vfsID,
				mock.DatabaseID,
				mock.DatabaseBranchID,
				"large_embeddings",
				"vector",
				queryVector,
				10,
				vector.MetricL2,
			)

			duration := time.Since(start)

			if err != nil {
				t.Fatalf("VectorScan failed: %v", err)
			}

			defer handle.Delete()

			scanHandle := handle.Value().(*database.VectorScanHandle)

			if len(scanHandle.Results) != 10 {
				t.Errorf("Expected 10 results, got %d", len(scanHandle.Results))
			}

			t.Logf("Parallel scan of %d vectors completed in %v", numVectors, duration)

			// Verify results are properly sorted
			for i := 1; i < len(scanHandle.Results); i++ {
				if scanHandle.Results[i].Distance < scanHandle.Results[i-1].Distance {
					t.Errorf("Results not sorted at index %d", i)
				}
			}
		})
	})
}

func TestCentralHeapMerging(t *testing.T) {
	t.Run("MergeMultipleBatchHeaps", func(t *testing.T) {
		centralHeap := vector.NewTopKHeap(5)

		// Simulate batch heaps coming from different chunks
		batch1 := vector.NewTopKHeap(3)
		batch1.Insert(1, 5.0)
		batch1.Insert(2, 3.0)
		batch1.Insert(3, 7.0)

		batch2 := vector.NewTopKHeap(3)
		batch2.Insert(4, 2.0)
		batch2.Insert(5, 9.0)
		batch2.Insert(6, 1.0)

		batch3 := vector.NewTopKHeap(3)
		batch3.Insert(7, 4.0)
		batch3.Insert(8, 6.0)
		batch3.Insert(9, 8.0)

		// Merge batches into central heap (simulating continuous merging)
		centralHeap.MergeWith(batch1)
		centralHeap.MergeWith(batch2)
		centralHeap.MergeWith(batch3)

		results := centralHeap.Results()

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

	t.Run("StreamingMerge", func(t *testing.T) {
		// Simulate the streaming merge pattern used in executeParallelScan
		centralHeap := vector.NewTopKHeap(3)
		streamChan := make(chan *database.VectorChunkResult, 10)

		// Start merger goroutine
		done := make(chan struct{})

		go func() {
			defer close(done)

			for batchResult := range streamChan {
				if batchResult.Error == nil && batchResult.Heap != nil {
					centralHeap.MergeWith(batchResult.Heap)
				}
			}
		}()

		// Send batch results
		batch1 := vector.NewTopKHeap(3)
		batch1.Insert(1, 5.0)
		batch1.Insert(2, 3.0)

		streamChan <- &database.VectorChunkResult{ChunkID: 0, Heap: batch1, Error: nil}

		batch2 := vector.NewTopKHeap(3)
		batch2.Insert(4, 2.0)
		batch2.Insert(6, 1.0)

		streamChan <- &database.VectorChunkResult{ChunkID: 1, Heap: batch2, Error: nil}

		close(streamChan)

		// Wait for merger to finish
		<-done

		results := centralHeap.Results()

		if len(results) != 3 {
			t.Errorf("Expected 3 results, got %d", len(results))
		}

		if results[0].RowId != 6 || results[0].Distance != 1.0 {
			t.Errorf("Best result should be rowid=6 dist=1.0, got rowid=%d dist=%f",
				results[0].RowId, results[0].Distance)
		}
	})

	t.Run("ConcurrentMerging", func(t *testing.T) {
		centralHeap := vector.NewTopKHeap(10)
		streamChan := make(chan *database.VectorChunkResult, 20)

		// Start merger goroutine
		done := make(chan struct{})

		go func() {
			defer close(done)

			for batchResult := range streamChan {
				if batchResult.Error == nil && batchResult.Heap != nil {
					centralHeap.MergeWith(batchResult.Heap)
				}
			}
		}()

		// Send multiple batches concurrently
		numBatches := 10

		for i := 0; i < numBatches; i++ {
			batch := vector.NewTopKHeap(5)
			batch.Insert(int64(i*10+1), float64(i)+1.0)
			batch.Insert(int64(i*10+2), float64(i)+2.0)
			streamChan <- &database.VectorChunkResult{ChunkID: i, Heap: batch, Error: nil}
		}

		close(streamChan)

		// Wait with timeout
		select {
		case <-done:
			// Success
		case <-time.After(2 * time.Second):
			t.Error("Timeout waiting for concurrent merging to complete")
		}

		results := centralHeap.Results()

		if len(results) != 10 {
			t.Errorf("Expected 10 results, got %d", len(results))
		}

		// Verify results are sorted by distance
		for i := 1; i < len(results); i++ {
			if results[i].Distance < results[i-1].Distance {
				t.Errorf("Results not sorted: result[%d].Distance=%f < result[%d].Distance=%f",
					i, results[i].Distance, i-1, results[i-1].Distance)
			}
		}
	})
}
