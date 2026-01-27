package vector

// Vector BLOB format version
const (
	VectorVersion1 byte = 0x01
)

// Vector type identifiers
const (
	VectorTypeFloat32 byte = 0x01
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
	return 4 // Default for now
}
