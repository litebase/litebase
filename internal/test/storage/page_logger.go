package storage

import (
	"github.com/litebase/litebase/pkg/memory"
	"github.com/litebase/litebase/pkg/storage"
)

// NewPageLoggerForTesting creates a new page logger without cluster coordination (for testing)
func NewPageLoggerForTesting(
	databaseId string,
	branchId string,
	networkFS *storage.FileSystem,
) (*storage.PageLogger, error) {
	// Create a memory manager with 100MB limit for testing
	memManager, err := memory.NewManager(memory.Config{
		Capacity:  100 * 1024 * 1024, // 100MB
		Threshold: 0.85,
	})

	if err != nil {
		return nil, err
	}

	return storage.NewPageLogger(databaseId, branchId, networkFS, NewMockNodePublisher(), memManager)
}
