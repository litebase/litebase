package storage

import (
	"errors"
	"log/slog"
	"maps"
	"sync"
)

type DataRangeManager struct {
	dfs        *DurableDatabaseFileSystem
	dataKey    []byte // Optional: encryption key (32 bytes)
	encrypted  bool   // Whether ranges are encrypted
	Index      *DataRangeIndex
	keyHash    [32]byte // Optional: SHA256 hash of encryption key
	mutex      *sync.RWMutex
	ranges     map[int64]*Range
	rangeUsage map[int64]int64

	lastRangeMap map[int64]int64
}

// Create a new instance of the data range manager.
func NewDataRangeManager(dfs *DurableDatabaseFileSystem) *DataRangeManager {
	drm := &DataRangeManager{
		dfs:          dfs,
		mutex:        &sync.RWMutex{},
		ranges:       make(map[int64]*Range),
		rangeUsage:   make(map[int64]int64),
		lastRangeMap: make(map[int64]int64),
	}

	drm.Index = NewDataRangeIndex(drm)

	return drm
}

// Acquire marks a range as being used at the specified timestamp.
func (drm *DataRangeManager) Acquire(timestamp int64) {
	drm.mutex.Lock()
	defer drm.mutex.Unlock()

	if _, ok := drm.rangeUsage[timestamp]; !ok {
		drm.rangeUsage[timestamp] = 0
	}

	drm.rangeUsage[timestamp]++
}

// Close closes all open ranges and the index file.
func (drm *DataRangeManager) Close() error {
	for _, r := range drm.ranges {
		if r != nil {
			err := r.Close()

			if err != nil {
				slog.Error("Error closing range", "error", err)
			}
		}
	}

	drm.ranges = make(map[int64]*Range)
	drm.rangeUsage = make(map[int64]int64)

	return nil
}

// Get retrieves a range at the specified timestamp, opening it if necessary.
func (drm *DataRangeManager) Get(rangeNumber int64) (*Range, error) {
	drm.mutex.Lock()
	defer drm.mutex.Unlock()

	// Get the range from the in-memory cache if it exists, and return the latest
	// version that is less than or equal to the requested timestamp.
	if r, ok := drm.ranges[rangeNumber]; ok {

		return r, nil
	}

	// Get the latest version of the range from the index.
	found, err := drm.Index.Get(rangeNumber)

	if err != nil {
		return nil, err
	}

	var r *Range

	// Create encrypted or unencrypted range based on configuration
	if drm.encrypted {
		r, err = NewEncryptedRange(
			drm.dfs.databaseId,
			drm.dfs.branchId,
			drm.dfs.tieredFS,
			rangeNumber,
			drm.dfs.pageSize,
			drm.dataKey,
			drm.keyHash,
		)
	} else {
		r, err = NewRange(
			drm.dfs.databaseId,
			drm.dfs.branchId,
			drm.dfs.tieredFS,
			rangeNumber,
			drm.dfs.pageSize,
		)
	}

	if err != nil {
		return nil, err
	}

	if !found {
		// Update the range index with the latest version.
		err = drm.Index.Set(rangeNumber, 0)

		if err != nil {
			// Close the range we just created since we can't index it
			if err := r.Close(); err != nil {
				slog.Error("failed to close range after index error", "error", err)
			}

			return nil, err
		}
	}

	drm.ranges[rangeNumber] = r

	return r, nil
}

// ConfigureEncryption sets the encryption parameters for this DataRangeManager.
// All subsequently created Ranges will be encrypted.
// This should be called immediately after DataRangeManager creation if encryption is needed.
func (drm *DataRangeManager) ConfigureEncryption(dataKey []byte, keyHash [32]byte) error {
	if len(dataKey) != 32 {
		return errors.New("dataKey must be exactly 32 bytes")
	}

	drm.dataKey = dataKey
	drm.encrypted = true
	drm.keyHash = keyHash

	return nil
}

// GetOldestTimestamp returns the oldest timestamp that is still in use.
func (drm *DataRangeManager) GetOldestTimestamp() int64 {
	drm.mutex.RLock()
	defer drm.mutex.RUnlock()

	return drm.getOldestTimestamp()
}

// getOldestTimestamp is the internal implementation of GetOldestTimestamp.
func (drm *DataRangeManager) getOldestTimestamp() int64 {
	if len(drm.rangeUsage) == 0 {
		return 0
	}

	var oldest int64
	first := true

	for timestamp := range drm.rangeUsage {
		if first || timestamp < oldest {
			oldest = timestamp
			first = false
		}
	}

	return oldest
}

// RangeUsage returns a copy of the current range usage map.
func (drm *DataRangeManager) RangeUsage() map[int64]int64 {
	drm.mutex.RLock()
	defer drm.mutex.RUnlock()

	usageCopy := make(map[int64]int64)

	maps.Copy(usageCopy, drm.rangeUsage)

	return usageCopy
}

// Release marks a range as no longer being used at the specified timestamp.
func (drm *DataRangeManager) Release(timestamp int64) {
	drm.mutex.Lock()
	defer drm.mutex.Unlock()

	drm.rangeUsage[timestamp] = drm.rangeUsage[timestamp] - 1

	if drm.rangeUsage[timestamp] <= 0 {
		delete(drm.rangeUsage, timestamp)
	}
}

// Remove deletes a range file at the specified timestamp.
func (drm *DataRangeManager) Remove(rangeNumber int64) error {
	drm.mutex.Lock()
	defer drm.mutex.Unlock()

	if _, ok := drm.ranges[rangeNumber]; !ok {
		return errors.New("range not found")
	}

	delete(drm.ranges, rangeNumber)

	return nil
}
