package storage

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/litebase/litebase/pkg/file"
)

// const PageLogManagerCompactionInterval = time.Second * 1
const PageLogManagerCompactionInterval = time.Second * 10

type PageLogManagerConfig func(*PageLogManager)

// The PageLogManager is responsible for managing page loggers and runnning
// compaction tasks for page logs. There should only be one PageLogManager per
// to avoid duplicate processing.
type PageLogManager struct {
	compacting         bool
	compactionFn       func()
	CompactionInterval time.Duration
	context            context.Context
	loggers            map[string]*PageLogger
	mutex              *sync.Mutex
	running            bool
}

// Create a new instance of the PageLogManager.
func NewPageLogManager(ctx context.Context, config ...PageLogManagerConfig) *PageLogManager {
	plm := &PageLogManager{
		CompactionInterval: PageLogManagerCompactionInterval,
		compactionFn:       func() {},
		context:            ctx,
		loggers:            make(map[string]*PageLogger),
		mutex:              &sync.Mutex{},
	}

	for _, cfg := range config {
		cfg(plm)
	}

	go plm.run()

	return plm
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

	logger, err := NewPageLogger(databaseId, branchId, networkFS)

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
			plm.Close()
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
