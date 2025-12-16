package database

import (
	"errors"
	"sync"

	"github.com/litebase/litebase/pkg/storage"
)

type DatabaseExportManager struct {
	branchId     string
	databaseId   string
	fileSystem   *storage.DurableDatabaseFileSystem
	mutex        sync.Mutex
	ActiveExport *DatabaseExport
}

// Create a new DatabaseExportManager instance.
func NewDatabaseExportManager(databaseId, branchId string, fileSystem *storage.DurableDatabaseFileSystem) *DatabaseExportManager {
	return &DatabaseExportManager{
		databaseId: databaseId,
		branchId:   branchId,
		fileSystem: fileSystem,
	}
}

// Clear the active database export.
func (m *DatabaseExportManager) Clear() {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.ActiveExport = nil
}

// Create a new database export and set it as the active export.
func (m *DatabaseExportManager) Create() (*DatabaseExport, error) {
	// Use TryLock to ensure only one export can be created at a time
	if !m.mutex.TryLock() {
		return nil, errors.New("another export is already in progress")
	}

	defer m.mutex.Unlock()

	// Check if there's already an active export
	if m.ActiveExport != nil {
		return nil, errors.New("an export is already active")
	}

	// Get all range entries from the range manager
	ranges, err := m.fileSystem.RangeManager.Index.All()

	if err != nil {
		return nil, err
	}

	// Create a new database export
	export := NewDatabaseExport(m.fileSystem, ranges)

	m.ActiveExport = export

	return export, nil
}

// Get the current active database export.
func (m *DatabaseExportManager) Get() (*DatabaseExport, error) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.ActiveExport == nil {
		return nil, errors.New("no active export")
	}

	return m.ActiveExport, nil
}
