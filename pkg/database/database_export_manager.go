package database

import (
	"errors"
	"sync"
	"time"

	"github.com/litebase/litebase/pkg/storage"
)

const ExportInactivityTimeout = 60 * time.Second

type DatabaseExportManager struct {
	branchId       string
	databaseId     string
	fileSystem     *storage.DurableDatabaseFileSystem
	mutex          sync.Mutex
	ActiveExport   *DatabaseExport
	lastActivity   time.Time
	cleanupTimer   *time.Timer
	barrierDone    chan struct{}
	barrierRelease chan struct{}
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

	if m.cleanupTimer != nil {
		m.cleanupTimer.Stop()
		m.cleanupTimer = nil
	}

	if m.ActiveExport != nil {
		m.ActiveExport.End()
		m.ActiveExport = nil
	}

	// Release the barrier if not already released
	if m.barrierRelease != nil {
		select {
		case <-m.barrierRelease:
			// Channel already closed, nothing to do
		default:
			close(m.barrierRelease)
		}
		m.barrierRelease = nil
	}

	barrierDone := m.barrierDone
	m.barrierDone = nil

	m.mutex.Unlock()

	// Wait for the barrier goroutine to finish (outside the lock to avoid deadlock)
	if barrierDone != nil {
		<-barrierDone
	}
}

// UpdateActivity updates the last activity timestamp and resets the cleanup timer.
func (m *DatabaseExportManager) UpdateActivity() {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	m.lastActivity = time.Now()

	// Reset the cleanup timer
	if m.cleanupTimer != nil {
		m.cleanupTimer.Stop()
	}

	m.cleanupTimer = time.AfterFunc(ExportInactivityTimeout, func() {
		m.Clear()
	})
}

// IsExpired checks if the export has expired due to inactivity.
func (m *DatabaseExportManager) IsExpired() bool {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.ActiveExport == nil {
		return true
	}

	return time.Since(m.lastActivity) > ExportInactivityTimeout
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

	// Set up a channel to hold the compaction barrier
	barrierRelease := make(chan struct{})
	barrierDone := make(chan struct{})

	// Start a goroutine that holds the compaction barrier until export is done
	barrierAcquired := make(chan struct{})

	go func() {
		defer close(barrierDone)

		err := m.fileSystem.CompactionBarrier(func() error {
			// Signal that we've acquired the barrier
			close(barrierAcquired)

			// Block here until the export is completed or cancelled
			<-barrierRelease
			return nil
		})

		if err != nil {
			select {
			case <-barrierAcquired:
				// Barrier was acquired, this shouldn't error
			default:
				// Signal that we failed to acquire (in case someone is waiting)
				close(barrierAcquired)
			}
		}
	}()

	// Wait for the barrier to be acquired or timeout
	select {
	case <-barrierAcquired:
		// Barrier acquired successfully
	case <-time.After(3 * time.Second):
		// Timeout - close release channel to stop the goroutine
		close(barrierRelease)
		// Wait for goroutine to finish
		<-barrierDone
		return nil, errors.New("timeout waiting for compaction barrier")
	}

	// Store the release channels so we can signal the barrier to release and wait for completion
	m.barrierRelease = barrierRelease
	m.barrierDone = barrierDone

	// Store the release function so we can signal the barrier to release
	export.releaseBarrier = func() {
		if m.barrierRelease != nil {
			close(m.barrierRelease)
			m.barrierRelease = nil
		}
	}

	m.ActiveExport = export
	m.lastActivity = time.Now()

	// Start cleanup timer
	m.cleanupTimer = time.AfterFunc(ExportInactivityTimeout, func() {
		m.Clear()
	})

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
