package database

import (
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/litebase/litebase/pkg/storage"
)

type DatabaseExport struct {
	ID             string
	fileSystem     *storage.DurableDatabaseFileSystem
	ranges         map[int64]storage.DataRangeIndexEntry
	mutex          *sync.Mutex
	StartedAt      time.Time
	CompletedAt    *time.Time
	releaseBarrier func()
}

func NewDatabaseExport(fileSystem *storage.DurableDatabaseFileSystem, ranges map[int64]storage.DataRangeIndexEntry) *DatabaseExport {
	return &DatabaseExport{
		ID:         uuid.NewString(),
		fileSystem: fileSystem,
		ranges:     ranges,
		mutex:      &sync.Mutex{},
		StartedAt:  time.Now(),
	}
}

func (de *DatabaseExport) End() {
	de.mutex.Lock()
	defer de.mutex.Unlock()

	now := time.Now()
	de.CompletedAt = &now

	// Release the compaction barrier
	if de.releaseBarrier != nil {
		de.releaseBarrier()
		de.releaseBarrier = nil
	}
}

func (de *DatabaseExport) GetRange(rangeNumber int64) (*storage.Range, error) {
	de.mutex.Lock()
	defer de.mutex.Unlock()

	// Check if the range exists in the export
	if _, exists := de.ranges[rangeNumber]; !exists {
		return nil, errors.New("range not found in export")
	}

	// Get the range from the file system
	return de.fileSystem.GetRangeFile(rangeNumber)
}

// Return the count of ranges in the export.
func (de *DatabaseExport) RangeCount() int {
	de.mutex.Lock()
	defer de.mutex.Unlock()

	return len(de.ranges)
}

// Return the list of range numbers in the export.
func (de *DatabaseExport) Ranges() []int {
	de.mutex.Lock()
	defer de.mutex.Unlock()

	rangeNumbers := make([]int, 0, len(de.ranges))

	for rangeNumber := range de.ranges {
		rangeNumbers = append(rangeNumbers, int(rangeNumber))
	}

	return rangeNumbers
}
