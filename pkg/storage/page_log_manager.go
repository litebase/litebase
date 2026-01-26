package storage

import (
	"context"
	"log"
	"log/slog"
	"maps"
	"sync"
	"time"

	"github.com/litebase/litebase/pkg/file"
	"github.com/litebase/litebase/pkg/memory"
)

// Currently using a lower time to catch bugs and issues
const PageLogManagerCompactionInterval = time.Second * 2

// Maximum number of databases that can run compaction simultaneously
const MaxConcurrentCompactions = 10

// const PageLogManagerCompactionInterval = time.Second * 10

type PageLogManagerConfig func(*PageLogManager)

// The PageLogManager is responsible for managing page loggers and running
// compaction tasks for page logs. There should only be one PageLogManager per
// to avoid duplicate processing.
type PageLogManager struct {
	compacting          bool
	compactionFn        func()
	compactionSemaphore chan struct{} // Limits concurrent compactions
	CompactionInterval  time.Duration
	context             context.Context
	loggers             map[string]*PageLogger
	memoryManager       *memory.Manager
	mutex               *sync.Mutex
	nodePublisher       NodePublisher
	running             bool
}

// Create a new instance of the PageLogManager.
func NewPageLogManager(ctx context.Context, memoryManager *memory.Manager, config ...PageLogManagerConfig) *PageLogManager {
	plm := &PageLogManager{
		CompactionInterval:  PageLogManagerCompactionInterval,
		compactionFn:        func() {},
		compactionSemaphore: make(chan struct{}, MaxConcurrentCompactions),
		context:             ctx,
		loggers:             make(map[string]*PageLogger),
		memoryManager:       memoryManager,
		mutex:               &sync.Mutex{},
	}

	for _, cfg := range config {
		cfg(plm)
	}

	go plm.run()

	return plm
}

// WithMaxConcurrentCompactions sets the maximum number of concurrent compactions
func WithMaxConcurrentCompactions(maxConcurrent int) PageLogManagerConfig {
	return func(plm *PageLogManager) {
		if maxConcurrent > 0 {
			plm.compactionSemaphore = make(chan struct{}, maxConcurrent)
		}
	}
}

// Close the PageLogManager and all its PageLogger instances.
func (plm *PageLogManager) Close() error {
	plm.mutex.Lock()
	defer plm.mutex.Unlock()

	for _, logger := range plm.loggers {
		err := logger.Close()

		if err != nil {
			return err
		}
	}

	plm.loggers = make(map[string]*PageLogger)

	return nil
}

// Get a page logger for a given database.
func (plm *PageLogManager) Get(
	databaseId string,
	branchId string,
	networkFS *FileSystem,
) *PageLogger {
	plm.mutex.Lock()
	defer plm.mutex.Unlock()

	key := file.DatabaseHash(databaseId, branchId)

	if logger, ok := plm.loggers[key]; ok {
		return logger
	}

	logger, err := NewPageLogger(
		databaseId,
		branchId,
		networkFS,
		plm.nodePublisher,
		plm.memoryManager,
	)

	if err != nil {
		log.Println("Error creating page logger", err)

		return nil
	}

	plm.loggers[key] = logger

	return plm.loggers[key]
}

// Release a logger for a given database.
func (plm *PageLogManager) Release(
	databaseId string,
	branchId string,
) error {
	plm.mutex.Lock()
	defer plm.mutex.Unlock()

	key := file.DatabaseHash(databaseId, branchId)

	if logger, ok := plm.loggers[key]; ok {
		err := logger.Close()

		if err != nil {
			return err
		}

		delete(plm.loggers, key)
	}

	return nil
}

// Run the compaction task periodically.
func (plm *PageLogManager) run() {
	if plm.running {
		return
	}

	plm.running = true

	defer func() {
		plm.running = false
	}()

	ticker := time.NewTicker(plm.CompactionInterval)
	defer ticker.Stop()

	for {
		select {
		case <-plm.context.Done():
			err := plm.Close()

			if err != nil {
				slog.Error("Error closing PageLogManager:", "error", err)
			}

			return
		case <-ticker.C:
			if plm.compacting {
				continue
			}

			plm.compacting = true
			plm.compactionFn()
			plm.compacting = false
		}
	}
}

// Set a function to be called for compaction tasks.
func (plm *PageLogManager) SetCompactionFn(
	fn func(),
) {
	plm.mutex.Lock()
	defer plm.mutex.Unlock()

	plm.compactionFn = fn
}

// CompactDatabase attempts to compact a specific database's PageLogger.
// Returns true if compaction was attempted, false if skipped due to concurrency limits.
func (plm *PageLogManager) CompactDatabase(
	databaseId string,
	branchId string,
	durableDatabaseFileSystem *DurableDatabaseFileSystem,
) (bool, error) {
	plm.mutex.Lock()
	key := file.DatabaseHash(databaseId, branchId)
	logger, exists := plm.loggers[key]
	plm.mutex.Unlock()

	if !exists {
		return false, nil // No logger for this database
	}

	// Try to acquire a compaction slot (non-blocking)
	select {
	case plm.compactionSemaphore <- struct{}{}:
		// Got a slot, proceed with compaction
		defer func() {
			<-plm.compactionSemaphore // Release the slot
		}()

		err := logger.Compact(durableDatabaseFileSystem)

		return true, err

	default:
		// All compaction slots are busy, skip this database for now
		slog.Debug("Skipping compaction for database due to concurrency limit",
			"databaseId", databaseId,
			"branchId", branchId,
			"activeCompactions", len(plm.compactionSemaphore),
			"maxConcurrent", MaxConcurrentCompactions)

		return false, nil
	}
}

// GetActiveCompactions returns the number of currently active compactions
func (plm *PageLogManager) GetActiveCompactions() int {
	return len(plm.compactionSemaphore)
}

// CompactAllDatabases attempts to compact all managed databases with concurrency control.
// This is intended to be used as the compaction function for the periodic ticker.
func (plm *PageLogManager) CompactAllDatabases(durableDatabaseFileSystemProvider func(databaseId, branchId string) *DurableDatabaseFileSystem) {
	plm.mutex.Lock()
	// Make a copy of the loggers map to avoid holding the lock during compaction
	loggersCopy := make(map[string]*PageLogger)
	maps.Copy(loggersCopy, plm.loggers)
	plm.mutex.Unlock()

	// Attempt to compact each database
	for _, logger := range loggersCopy {
		// Extract database and branch IDs from the logger
		databaseId := logger.DatabaseID
		branchId := logger.BranchID

		if durableDatabaseFileSystemProvider != nil {
			dfs := durableDatabaseFileSystemProvider(databaseId, branchId)

			if dfs != nil {
				attempted, err := plm.CompactDatabase(databaseId, branchId, dfs)

				if err != nil {
					slog.Error("Compaction failed during periodic compaction",
						"databaseId", databaseId,
						"branchId", branchId,
						"error", err)
				} else if !attempted {
					slog.Debug("Compaction skipped during periodic compaction due to concurrency limit",
						"databaseId", databaseId,
						"branchId", branchId)
				}
			}
		}
	}
}
