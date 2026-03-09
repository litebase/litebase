package vector

import "sync"

// VectorIndexManagerInterface defines the interface for the vector index
// manager used by C-level callbacks (goTriggerClusterSplits) to schedule
// cluster splits on the warm-cache connection after xCommit.
type VectorIndexManagerInterface interface {
	// RunSplitsWithConnection splits oversized clusters on the given connection.
	// conn is *database.DatabaseConnection passed as any to avoid import cycles.
	// Called by post-commit hooks so splits run on the warm-cache connection.
	RunSplitsWithConnection(conn any, databaseID, branchID, tableName string)
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
