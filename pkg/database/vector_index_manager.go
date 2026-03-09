package database

import (
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// IndexInfo tracks deduplication state for a vector index.
type IndexInfo struct {
	DatabaseID  string
	BranchID    string
	TableName   string
	LastUpdated time.Time
	Processing  bool
}

// VectorIndexManager deduplicates and executes vector cluster splits.
// It is set as the global index manager via vector.SetGlobalIndexManager
// so that C-level xCommit callbacks can schedule splits via post-commit
// hooks. The actual split work runs on the same warm-cache
// DatabaseConnection that performed the insert transaction.
type VectorIndexManager struct {
	indexes map[string]*IndexInfo // key: "dbID:branchID:tableName"
	mutex   *sync.RWMutex
}

// NewVectorIndexManager creates a new VectorIndexManager.
func NewVectorIndexManager() *VectorIndexManager {
	return &VectorIndexManager{
		indexes: make(map[string]*IndexInfo),
		mutex:   &sync.RWMutex{},
	}
}

// RunSplitsWithConnection splits oversized clusters on the given connection.
// conn is *DatabaseConnection passed as any to avoid import cycles in the
// post-commit hook system. This is the entry point used by
// goTriggerClusterSplits → PostCommitHook → this method.
func (vm *VectorIndexManager) RunSplitsWithConnection(conn any, databaseID, branchID, tableName string) {
	dbConn := conn.(*DatabaseConnection)

	vm.RunSplitsOnConnection(dbConn, databaseID, branchID, tableName)
}

// RunSplitsOnConnection splits oversized clusters using an existing connection
// whose page cache is warm from recent inserts. This avoids the expensive WAL
// re-read that occurs when a fresh connection is obtained from the pool.
//
// Called automatically by post-commit hooks registered in xCommit, so splits
// run on the same connection that performed the insert transaction.
//
// The databaseID and branchID are used for deduplication: while this method
// runs, any concurrent call for the same index will no-op.
func (vm *VectorIndexManager) RunSplitsOnConnection(dbConn *DatabaseConnection, databaseID, branchID, tableName string) {
	key := vm.getKey(databaseID, branchID, tableName)

	vm.mutex.Lock()

	info, exists := vm.indexes[key]

	if !exists {
		info = &IndexInfo{
			DatabaseID:  databaseID,
			BranchID:    branchID,
			TableName:   tableName,
			LastUpdated: time.Now().UTC(),
		}
		vm.indexes[key] = info
	}

	if info.Processing {
		// Another split is already running for this index — let it finish.
		vm.mutex.Unlock()
		return
	}

	info.Processing = true
	info.LastUpdated = time.Now().UTC()
	vm.mutex.Unlock()

	defer func() {
		vm.mutex.Lock()

		if i, ok := vm.indexes[key]; ok {
			i.Processing = false
			i.LastUpdated = time.Now().UTC()
		}

		vm.mutex.Unlock()
	}()

	vm.runSplitsOnConnection(dbConn, tableName)
}

// runSplitsOnConnection performs the actual split work on a connection.
func (vm *VectorIndexManager) runSplitsOnConnection(dbConn *DatabaseConnection, tableName string) {
	vectorColumns, err := GetVectorColumns(dbConn, tableName)

	if err != nil {
		slog.Error("RunSplitsOnConnection: failed to get vector columns",
			"table", tableName, "error", err)
		return
	}

	res, err := dbConn.Exec(
		fmt.Sprintf(`SELECT key, value FROM %s_metadata WHERE key IN ('max_cluster_size', 'min_cluster_size')`, tableName),
		nil,
	)

	if err != nil || res == nil || len(res.Rows) == 0 {
		if res != nil {
			dbConn.ResultPool().Put(res)
		}

		return
	}

	metadata := make(map[string]string)

	for _, row := range res.Rows {
		metadata[string(row[0].Text())] = string(row[1].Text())
	}

	dbConn.ResultPool().Put(res)

	var maxClusterSize, minClusterSize int

	fmt.Sscanf(metadata["max_cluster_size"], "%d", &maxClusterSize)
	fmt.Sscanf(metadata["min_cluster_size"], "%d", &minClusterSize)

	if maxClusterSize <= 0 {
		return
	}

	indexer, err := NewVectorIndexer(dbConn, tableName, vectorColumns, maxClusterSize, minClusterSize)

	if err != nil {
		slog.Error("RunSplitsOnConnection: failed to create indexer", "table", tableName, "error", err)
		return
	}

	ctx := dbConn.Context()

	for _, col := range vectorColumns {
		if err := indexer.SplitOversizedClusters(ctx, col.Name, col.DistanceMetric); err != nil {
			slog.Error("RunSplitsOnConnection: split error",
				"table", tableName, "col", col.Name, "error", err)
		}
	}
}

func (vm *VectorIndexManager) getKey(databaseID, branchID, tableName string) string {
	return databaseID + ":" + branchID + ":" + tableName
}
