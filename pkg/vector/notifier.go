package vector

import "sync"

// VectorIndexManagerInterface defines the interface needed from server.VectorIndexManager
type VectorIndexManagerInterface interface {
	MarkPending(databaseID, branchID, tableName string)
	// PERF_TEST: inline processing paths — called synchronously / immediately after commit.
	ProcessInline(databaseID, branchID, tableName string)
	// RunSplits splits oversized clusters after the batch commit.
	RunSplits(databaseID, branchID, tableName string)
}

var (
	globalIndexManager     VectorIndexManagerInterface
	globalIndexManagerLock sync.RWMutex
)

// SetGlobalIndexManager sets the global index manager instance
func SetGlobalIndexManager(mgr VectorIndexManagerInterface) {
	globalIndexManagerLock.Lock()
	defer globalIndexManagerLock.Unlock()

	globalIndexManager = mgr
}

// GetGlobalIndexManager returns the global index manager instance
func GetGlobalIndexManager() VectorIndexManagerInterface {
	globalIndexManagerLock.RLock()
	defer globalIndexManagerLock.RUnlock()

	return globalIndexManager
}

// NotifyVectorInsert is called when a vector is inserted to trigger indexing
func NotifyVectorInsert(databaseID, branchID, tableName string) {
	mgr := GetGlobalIndexManager()

	if mgr != nil {
		mgr.MarkPending(databaseID, branchID, tableName)
	}
}

// PERF_TEST: ProcessVectorInsertInline is a no-op now that goAssignVectorsInBatch
// assigns every vector to the correct cluster inside flush_insert_buffer.
// cluster_id=0 is never written, so there is nothing to process asynchronously.
func ProcessVectorInsertInline(databaseID, branchID, tableName string) {}
