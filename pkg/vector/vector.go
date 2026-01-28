package vector

import "runtime"

// Vector BLOB format version
const (
	VectorVersion1 byte = 0x01
)

// Vector type identifiers
const (
	VectorTypeFloat32 byte = 0x01
	VectorTypeFloat64 byte = 0x02
	VectorTypeInt8    byte = 0x03
	VectorTypeInt16   byte = 0x04
	VectorTypeFloat16 byte = 0x05 // Half-precision (2 bytes/dim)
	VectorTypeBit     byte = 0x06 // Binary quantization (1 bit/dim)
	VectorTypeSparse  byte = 0x07 // Sparse vectors (index-value pairs)
)

// Maximum dimensions supported
const MaxDimensions = 4096

// Global worker pool instance
var globalWorkerPool *WorkerPool

// GetWorkerPool returns the global worker pool instance, creating it if needed
func GetWorkerPool() *WorkerPool {
	if globalWorkerPool == nil {
		maxWorkers := 2 * getNumCPU()
		globalWorkerPool = NewWorkerPool(maxWorkers)
	}

	return globalWorkerPool
}

// ShutdownWorkerPool stops the global worker pool
func ShutdownWorkerPool() {
	if globalWorkerPool != nil {
		globalWorkerPool.Shutdown()
		globalWorkerPool = nil
	}
}

// getNumCPU returns the number of available CPUs
func getNumCPU() int {
	// This will be implemented to get runtime.NumCPU()
	numCPUs := runtime.NumCPU()

	if numCPUs > 0 {
		return numCPUs
	}

	return 4
}
