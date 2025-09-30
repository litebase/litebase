package storage

import "github.com/litebase/litebase/pkg/storage"

// NewPageLoggerForTesting creates a new page logger without cluster coordination (for testing)
func NewPageLoggerForTesting(
	databaseId string,
	branchId string,
	networkFS *storage.FileSystem,
) (*storage.PageLogger, error) {
	return storage.NewPageLogger(databaseId, branchId, networkFS, NewMockNodePublisher())
}
