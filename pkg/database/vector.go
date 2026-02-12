package database

import "runtime"

// Global worker pool instance
var globalWorkerPool *VectorWorkerPool

// GetWorkerPool returns the global worker pool instance, creating it if needed
func GetWorkerPool() *VectorWorkerPool {
	if globalWorkerPool == nil {
		maxWorkers := 2 * getNumCPU()
		globalWorkerPool = NewVectorWorkerPool(maxWorkers)
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
