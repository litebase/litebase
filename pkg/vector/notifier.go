package vector

import "sync"

// VectorIndexManagerInterface defines the interface needed from server.VectorIndexManager
type VectorIndexManagerInterface interface {
	MarkPending(databaseID, branchID, tableName string)
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
