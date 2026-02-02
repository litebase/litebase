package server

import (
	"context"
	"log/slog"
	"sync"
	"time"
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
		return
	}

	vm.mutex.Lock()
	defer vm.mutex.Unlock()

	now := time.Now().UTC()

	for _, info := range vm.indexes {
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
				continue
			}
		}

		// Skip if no pending vectors
		if info.PendingCount == 0 {
			continue
		}

		// Enqueue indexing job
		jobData := map[string]interface{}{
			"db_id":      info.DatabaseID,
			"branch_id":  info.BranchID,
			"table_name": info.TableName,
		}

		if vm.app.Cluster.Node().Context().Err() != nil {
			// App is shutting down, skip dispatching new jobs
			continue
		}

		_, err := vm.app.QueueDispatcher.DispatchJob("VectorIndexer", jobData)

		if err != nil {
			slog.Error("Failed to dispatch VectorIndexer job",
				"database", info.DatabaseID,
				"branch", info.BranchID,
				"table", info.TableName,
				"error", err)
			continue
		}

		slog.Debug("Dispatched VectorIndexer job",
			"database", info.DatabaseID,
			"branch", info.BranchID,
			"table", info.TableName,
			"pending_count", info.PendingCount)

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

// ProcessIndexesForTest exposes processIndexes for testing
func (vm *VectorIndexManager) ProcessIndexesForTest() {
	vm.processIndexes()
}

// GetContext returns the context for testing
func (vm *VectorIndexManager) GetContext() context.Context {
	return vm.context
}
