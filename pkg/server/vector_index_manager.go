package server

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/litebase/litebase/pkg/database"
)

var (
	IndexManagerTickInterval      = 100 * time.Millisecond
	IndexManagerProcessingTimeout = 5 * time.Minute
)

// IndexInfo tracks pending state for a vector index
type IndexInfo struct {
	DatabaseID   string
	BranchID     string
	TableName    string
	PendingCount int64
	LastUpdated  time.Time
	Processing   bool
}

// VectorIndexManager monitors vector indexes and triggers indexing jobs
type VectorIndexManager struct {
	app         *App
	context     context.Context
	cancel      context.CancelFunc
	indexes     map[string]*IndexInfo // key: "dbID:branchID:tableName"
	mutex       *sync.RWMutex
	triggerChan chan struct{}
	wg          sync.WaitGroup
}

func NewVectorIndexManager(app *App) *VectorIndexManager {
	ctx, cancel := context.WithCancel(app.Cluster.Node().Context())

	return &VectorIndexManager{
		app:         app,
		context:     ctx,
		cancel:      cancel,
		indexes:     make(map[string]*IndexInfo),
		mutex:       &sync.RWMutex{},
		triggerChan: make(chan struct{}, 100),
	}
}

// MarkPending marks an index as having pending vectors to process
func (vm *VectorIndexManager) MarkPending(databaseID, branchID, tableName string) {
	key := vm.getKey(databaseID, branchID, tableName)

	vm.mutex.Lock()
	defer vm.mutex.Unlock()

	if info, exists := vm.indexes[key]; exists {
		info.PendingCount++
		info.LastUpdated = time.Now().UTC()
	} else {
		vm.indexes[key] = &IndexInfo{
			DatabaseID:   databaseID,
			BranchID:     branchID,
			TableName:    tableName,
			PendingCount: 1,
			LastUpdated:  time.Now().UTC(),
			Processing:   false,
		}
	}

	// Trigger immediate processing (non-blocking)
	select {
	case vm.triggerChan <- struct{}{}:
	default:
		// Channel full, processing already triggered
	}
}

// Run starts the monitoring loop
func (vm *VectorIndexManager) Run() {
	vm.wg.Add(1)
	defer vm.wg.Done()

	ticker := time.NewTicker(IndexManagerTickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-vm.context.Done():
			return
		case <-vm.triggerChan:
			// Immediate trigger from MarkPending
			vm.processIndexes()
		case <-ticker.C:
			// Periodic check for any missed updates
			vm.processIndexes()
		}
	}
}

// processIndexes checks for indexes that need processing and enqueues jobs
func (vm *VectorIndexManager) processIndexes() {
	// Only process on primary node
	if !vm.app.Cluster.Node().IsPrimary() {
		slog.Debug("Skipping processIndexes - not primary node")
		return
	}

	vm.mutex.Lock()
	defer vm.mutex.Unlock()

	now := time.Now().UTC()

	for key, info := range vm.indexes {
		slog.Debug("Processing index",
			"key", key,
			"processing", info.Processing,
			"pending_count", info.PendingCount,
			"last_updated", info.LastUpdated)

		// Skip if already processing
		if info.Processing {
			// Check for stuck processing (timeout after 5 minutes)
			if now.Sub(info.LastUpdated) > IndexManagerProcessingTimeout {
				slog.Warn("Vector index processing timeout, resetting",
					"database", info.DatabaseID,
					"branch", info.BranchID,
					"table", info.TableName,
					"elapsed", now.Sub(info.LastUpdated))
				info.Processing = false
			} else {
				slog.Debug("Skipping index - already processing",
					"key", key,
					"elapsed", now.Sub(info.LastUpdated))
				continue
			}
		}

		// Skip if no pending vectors
		if info.PendingCount == 0 {
			slog.Debug("Skipping index - no pending vectors", "key", key)
			continue
		}

		// Mark as processing and reset count
		info.Processing = true
		info.PendingCount = 0
		info.LastUpdated = now
	}

	// Clean up processed indexes that haven't been updated in a while
	for key, info := range vm.indexes {
		if !info.Processing && info.PendingCount == 0 && now.Sub(info.LastUpdated) > 5*time.Minute {
			delete(vm.indexes, key)
		}
	}
}

// ProcessInline synchronously indexes all cluster-0 vectors for the given table.
// PERF_TEST: this is the inline path used instead of the async background job.
// Called from a goroutine fired immediately after each batch commit, so that
// processing starts right away without queue dispatch or debounce latency.
// If a ProcessInline goroutine is already running for this index, the call is a
// no-op — the running goroutine will drain all cluster-0 vectors on its own.
func (vm *VectorIndexManager) ProcessInline(databaseID, branchID, tableName string) {
	if !vm.app.Cluster.Node().IsPrimary() {
		return
	}

	// Deduplicate: only one ProcessInline runs per index at a time.
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
		// Another goroutine is already draining this index; let it finish.
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

	conn, err := vm.app.DatabaseManager.ConnectionManager().Get(databaseID, branchID)

	if err != nil {
		slog.Error("ProcessInline: failed to get connection",
			"db_id", databaseID,
			"branch_id", branchID,
			"table", tableName,
			"error", err)
		return
	}

	defer vm.app.DatabaseManager.ConnectionManager().Release(conn)

	dbConn := conn.GetConnection()

	vectorColumns, err := database.GetVectorColumns(dbConn, tableName)

	if err != nil {
		slog.Error("ProcessInline: failed to get vector columns",
			"db_id", databaseID,
			"table", tableName,
			"error", err)
		return
	}

	res, err := dbConn.Exec(
		fmt.Sprintf(`SELECT key, value FROM %s_metadata WHERE key IN ('max_cluster_size', 'min_cluster_size')`, tableName),
		nil,
	)

	if err != nil || len(res.Rows) == 0 {
		slog.Error("ProcessInline: failed to read index config",
			"table", tableName,
			"error", err)
		return
	}

	metadata := make(map[string]string)

	for _, row := range res.Rows {
		metadata[string(row[0].Text())] = string(row[1].Text())
	}

	var maxClusterSize, minClusterSize int

	fmt.Sscanf(metadata["max_cluster_size"], "%d", &maxClusterSize)
	fmt.Sscanf(metadata["min_cluster_size"], "%d", &minClusterSize)

	indexer, err := database.NewVectorIndexer(dbConn, tableName, vectorColumns, maxClusterSize, minClusterSize)

	if err != nil {
		slog.Error("ProcessInline: failed to create indexer",
			"table", tableName,
			"error", err)
		return
	}

	ctx := vm.app.Cluster.Node().Context()
	totalProcessed := 0

	for {
		processed, err := indexer.ProcessBatch(ctx, VectorIndexerBatchSize)

		if err != nil {
			slog.Error("ProcessInline: batch error",
				"db_id", databaseID,
				"table", tableName,
				"processed", totalProcessed,
				"error", err)
			return
		}

		totalProcessed += processed

		if processed < VectorIndexerBatchSize {
			slog.Debug("ProcessInline: completed",
				"db_id", databaseID,
				"branch_id", branchID,
				"table", tableName,
				"total_processed", totalProcessed)
			return
		}
	}
}

// RunSplits splits oversized clusters for the given index using a separate
// connection (safe to call from a goroutine after the insert transaction commits).
// Only one RunSplits runs per index at a time to avoid concurrent split storms.
func (vm *VectorIndexManager) RunSplits(databaseID, branchID, tableName string) {
	if !vm.app.Cluster.Node().IsPrimary() {
		return
	}

	// Deduplicate: only one RunSplits runs per index at a time.
	// If a split is already running for this index, the call is a no-op —
	// the running goroutine will perform all necessary splits on its own pass.
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

	conn, err := vm.app.DatabaseManager.ConnectionManager().Get(databaseID, branchID)

	if err != nil {
		slog.Error("RunSplits: failed to get connection",
			"db_id", databaseID, "branch_id", branchID, "table", tableName, "error", err)
		return
	}

	defer vm.app.DatabaseManager.ConnectionManager().Release(conn)

	dbConn := conn.GetConnection()

	vectorColumns, err := database.GetVectorColumns(dbConn, tableName)

	if err != nil {
		slog.Error("RunSplits: failed to get vector columns",
			"db_id", databaseID, "table", tableName, "error", err)
		return
	}

	res, err := dbConn.Exec(
		fmt.Sprintf(`SELECT key, value FROM %s_metadata WHERE key IN ('max_cluster_size', 'min_cluster_size')`, tableName),
		nil,
	)

	if err != nil || len(res.Rows) == 0 {
		return
	}

	metadata := make(map[string]string)

	for _, row := range res.Rows {
		metadata[string(row[0].Text())] = string(row[1].Text())
	}

	var maxClusterSize, minClusterSize int

	fmt.Sscanf(metadata["max_cluster_size"], "%d", &maxClusterSize)
	fmt.Sscanf(metadata["min_cluster_size"], "%d", &minClusterSize)

	if maxClusterSize <= 0 {
		return
	}

	indexer, err := database.NewVectorIndexer(dbConn, tableName, vectorColumns, maxClusterSize, minClusterSize)

	if err != nil {
		slog.Error("RunSplits: failed to create indexer", "table", tableName, "error", err)
		return
	}

	ctx := vm.app.Cluster.Node().Context()

	for _, col := range vectorColumns {
		if err := indexer.SplitOversizedClusters(ctx, col.Name, col.DistanceMetric); err != nil {
			slog.Error("RunSplits: split error",
				"db_id", databaseID, "table", tableName, "col", col.Name, "error", err)
		}
	}
}

// MarkProcessed marks an index as finished processing
func (vm *VectorIndexManager) MarkProcessed(databaseID, branchID, tableName string) {
	key := vm.getKey(databaseID, branchID, tableName)

	vm.mutex.Lock()
	defer vm.mutex.Unlock()

	if info, exists := vm.indexes[key]; exists {
		info.Processing = false
		info.LastUpdated = time.Now().UTC()
	}
}

// Shutdown stops the manager
func (vm *VectorIndexManager) Shutdown() {
	vm.cancel()
	vm.wg.Wait()
}

func (vm *VectorIndexManager) getKey(databaseID, branchID, tableName string) string {
	return databaseID + ":" + branchID + ":" + tableName
}

// Test helpers - exposed for testing

// GetIndexes returns a copy of the indexes map for testing
func (vm *VectorIndexManager) GetIndexes() map[string]*IndexInfo {
	vm.mutex.RLock()
	defer vm.mutex.RUnlock()

	copy := make(map[string]*IndexInfo, len(vm.indexes))

	for k, v := range vm.indexes {
		copy[k] = v
	}

	return copy
}

// GetContext returns the context for testing
func (vm *VectorIndexManager) GetContext() context.Context {
	return vm.context
}

// ProcessIndexesForTest manually triggers index processing for testing
func (vm *VectorIndexManager) ProcessIndexesForTest() {
	vm.processIndexes()
}
