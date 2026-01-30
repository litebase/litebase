package vector

/*
#include <stdlib.h>
*/
import "C"

import (
	"fmt"
	"log/slog"
	"runtime/cgo"
	"unsafe"
)

// ScanHandle holds the results of a vector scan
type ScanHandle struct {
	Results []VectorResult
	Index   int
}

// VectorScan performs a parallel k-NN vector search
func VectorScan(vfsID, databaseID, branchID, tableName, columnName string, queryBlob []byte, k int, metric string) (cgo.Handle, error) {
	// Parse query vector
	queryVector, err := ParseVectorBlob(queryBlob)

	if err != nil {
		return 0, fmt.Errorf("failed to parse query vector: %w", err)
	}

	// Execute parallel scan
	results, err := executeParallelScan(vfsID, databaseID, branchID, tableName, columnName, queryVector, k, metric)

	if err != nil {
		return 0, err
	}

	// Create handle
	handle := &ScanHandle{
		Results: results,
		Index:   0,
	}

	return cgo.NewHandle(handle), nil
}

// executeParallelScan executes the parallel scan across table partitions
func executeParallelScan(vfsID, databaseID, branchID, tableName, columnName string, queryVector *VectorBlob, k int, metric string) ([]VectorResult, error) {
	// Partition the table
	partitions, err := PartitionTable(vfsID, databaseID, branchID, tableName, columnName, queryVector, k, metric)

	if err != nil {
		return nil, fmt.Errorf("failed to partition table: %w", err)
	}

	if len(partitions) == 0 {
		return []VectorResult{}, nil
	}

	// Get worker pool
	pool := GetWorkerPool()

	// Phase 2.5: Create streaming channel for batch heaps
	// Buffer size = workers * batches per chunk (approx 10 batches/chunk)
	streamChan := make(chan *ChunkResult, pool.MaxWorkers()*10)
	resultChan := make(chan *ChunkResult, len(partitions))

	// Create central heap for continuous merging
	centralHeap := NewTopKHeap(k)

	// Start central merger goroutine that continuously merges incoming batch heaps
	mergerDone := make(chan struct{})

	go func() {
		defer close(mergerDone)

		for batchResult := range streamChan {
			if batchResult.Error != nil {
				slog.Debug("Batch error", "chunk_id", batchResult.ChunkID, "error", batchResult.Error)
				continue
			}

			// Merge batch heap into central heap as it arrives
			// This overlaps merge work with database I/O from workers
			centralHeap.MergeWith(batchResult.Heap)
		}
	}()

	// Submit jobs to worker pool with streaming channel
	for i, partition := range partitions {
		job := &ChunkJob{
			ChunkID:     i,
			StartRow:    partition.StartRow,
			EndRow:      partition.EndRow,
			VfsID:       vfsID,
			DatabaseID:  databaseID,
			BranchID:    branchID,
			TableName:   tableName,
			ColumnName:  columnName,
			QueryVector: queryVector,
			Metric:      metric,
			K:           k,
			ResultChan:  resultChan,
			StreamChan:  streamChan, // Phase 2.5: Stream batch heaps here
		}

		pool.Submit(job)
	}

	// Wait for all chunks to complete
	for i := 0; i < len(partitions); i++ {
		result := <-resultChan

		if result.Error != nil {
			slog.Debug("Chunk scan error", "chunk_id", result.ChunkID, "error", result.Error)
		}
	}

	close(resultChan)
	close(streamChan) // Signal merger we're done streaming

	// Wait for merger to finish processing all batches
	<-mergerDone

	// Return sorted results from central heap
	return centralHeap.Results(), nil
}

// CGO exports for C code

//export goVectorScan
func goVectorScan(vfsID, databaseID, branchID, tableName, columnName *C.char, queryBlob unsafe.Pointer, queryBlobLen C.int, k C.int, metric *C.char) C.longlong {
	return GoVectorScan(vfsID, databaseID, branchID, tableName, columnName, queryBlob, queryBlobLen, k, metric)
}

// GoVectorScan is the exported Go function that can be called from other packages
func GoVectorScan(vfsID, databaseID, branchID, tableName, columnName *C.char, queryBlob unsafe.Pointer, queryBlobLen C.int, k C.int, metric *C.char) C.longlong {
	vfsIDStr := C.GoString(vfsID)
	databaseIDStr := C.GoString(databaseID)
	branchIDStr := C.GoString(branchID)
	tableNameStr := C.GoString(tableName)
	columnNameStr := C.GoString(columnName)
	metricStr := C.GoString(metric)

	queryBlobBytes := C.GoBytes(queryBlob, queryBlobLen)

	handle, err := VectorScan(vfsIDStr, databaseIDStr, branchIDStr, tableNameStr, columnNameStr, queryBlobBytes, int(k), metricStr)

	if err != nil {
		slog.Error("Vector scan failed", "error", err)
		return 0
	}

	return C.longlong(handle)
}

//export goGetScanResult
func goGetScanResult(handleID C.longlong, rowid *C.longlong, distance *C.double) C.int {
	return GoGetScanResult(handleID, rowid, distance)
}

// GoGetScanResult is the exported Go function that can be called from other packages
func GoGetScanResult(handleID C.longlong, rowid *C.longlong, distance *C.double) C.int {
	handle := cgo.Handle(handleID).Value().(*ScanHandle)

	if handle.Index >= len(handle.Results) {
		return 0
	}

	result := handle.Results[handle.Index]
	*rowid = C.longlong(result.RowId)
	*distance = C.double(result.Distance)
	handle.Index++

	return 1
}

//export goReleaseScanResults
func goReleaseScanResults(handleID C.longlong) {
	GoReleaseScanResults(handleID)
}

// GoReleaseScanResults is the exported Go function that can be called from other packages
func GoReleaseScanResults(handleID C.longlong) {
	handle := cgo.Handle(handleID)
	handle.Delete()
}
