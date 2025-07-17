package storage

import (
	"errors"
	"io"
	"log"
	"log/slog"
	"maps"
	"slices"
	"sync"
)

type DataRangeManager struct {
	dfs        *DurableDatabaseFileSystem
	index      *DataRangeIndex
	mutex      *sync.RWMutex
	ranges     map[int64]map[int64]*Range
	rangeUsage map[int64]int64

	lastRangeMap map[int64]int64
}

// Create a new instance of the data range manager.
func NewDataRangeManager(dfs *DurableDatabaseFileSystem) *DataRangeManager {
	drm := &DataRangeManager{
		dfs:          dfs,
		mutex:        &sync.RWMutex{},
		ranges:       make(map[int64]map[int64]*Range),
		rangeUsage:   make(map[int64]int64),
		lastRangeMap: make(map[int64]int64),
	}

	drm.index = NewDataRangeIndex(drm)

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
	err := drm.index.Close()

	if err != nil {
		slog.Error(`Error closing range index`, "error", err)
	}

	for _, rangeVersions := range drm.ranges {
		for _, r := range rangeVersions {
			if r != nil {
				err := r.Close()

				if err != nil {
					slog.Error("Error closing range file", "error", err)
				}
			}
		}
	}

	drm.ranges = make(map[int64]map[int64]*Range)
	drm.rangeUsage = make(map[int64]int64)

	return nil
}

// Copy the latest version of a range and create a new version of a range file.
// This is called when the page logger compacts data into range files.
func (drm *DataRangeManager) CopyRange(rangeNumber int64, newTimestamp int64, fn func(newRange *Range) error) (*Range, error) {
	found, rangeTimestamp, err := drm.index.Get(rangeNumber)

	if err != nil {
		return nil, err
	}

	defer func() {
		drm.lastRangeMap[rangeNumber] = rangeTimestamp
	}()

	if drm.lastRangeMap[rangeNumber] != 0 && drm.lastRangeMap[rangeNumber] >= rangeTimestamp {
		panic("CopyRange: corrupted range index")
	}

	slog.Debug("CopyRange", "rangeTimestamp", rangeTimestamp, "lastRangeTimestamp", drm.lastRangeMap[rangeNumber])

	if !found {
		return nil, errors.New("range not found")
	}

	// existingRange, err := drm.Get(rangeNumber, boundaryTimestamp)
	existingRange, err := drm.Get(rangeNumber, rangeTimestamp)

	if err != nil {
		return nil, err
	}

	if newTimestamp <= existingRange.Timestamp {
		return nil, errors.New("new timestamp must be greater than boundary timestamp")
	}

	drm.mutex.Lock()
	defer drm.mutex.Unlock()

	// Create a new range with the provided timestamp
	newRange, err := NewRange(
		drm.dfs.databaseId,
		drm.dfs.branchId,
		drm.dfs.tieredFS,
		rangeNumber,
		drm.dfs.pageSize,
		newTimestamp, // Ensure new timestamp is greater than existing
	)

	if err != nil {
		return nil, err
	}

	if existingRange.Timestamp == newRange.Timestamp {
		panic("CopyRange: existing and new range timestamps are the same")
	}

	// Check if the files are the same
	if existingRange.file == newRange.file {
		panic("CopyRange: existing and new range files are the same")
	}

	existingRange.file.Sync()
	newRange.file.Seek(0, io.SeekStart)
	existingRange.file.Seek(0, io.SeekStart)

	// Verify positions are actually at 0
	// newPos, _ := newRange.file.Seek(0, io.SeekCurrent)
	// existingPos, _ := existingRange.file.Seek(0, io.SeekCurrent)

	// slog.Debug("File positions before copy",
	// 	"newPos", newPos,
	// 	"existingPos", existingPos)

	// Copy data from the existing range to the new range
	_, err = io.Copy(newRange.file, existingRange.file)

	if err != nil {
		return nil, err
	}

	// newRange.file.Sync()
	newRangeSize, _ := newRange.Size()
	existingRangeSize, _ := existingRange.Size()

	if newRangeSize != existingRangeSize {
		slog.Debug("CopyRange: size mismatch", "existingSize", existingRangeSize, "newSize", newRangeSize)
		panic("CopyRange: new range size does not match existing range size")
	}

	// Call the provided function to allow further modifications to the new range
	if fn != nil {
		err = fn(newRange)

		if err != nil {
			return nil, err
		}
	}

	// Update the range index with the new version
	err = drm.index.Set(rangeNumber, newRange.Timestamp)

	if err != nil {
		return nil, err
	}

	// Ensure the map for this range number exists
	if _, ok := drm.ranges[rangeNumber]; !ok {
		drm.ranges[rangeNumber] = make(map[int64]*Range)
	}

	// Store the new range in the in-memory cache
	drm.ranges[rangeNumber][newRange.Timestamp] = newRange

	return newRange, nil
}

// Get retrieves a range at the specified timestamp, opening it if necessary.
func (drm *DataRangeManager) Get(rangeNumber int64, timestamp int64) (*Range, error) {
	drm.mutex.Lock()
	defer drm.mutex.Unlock()

	// Get the range from the in-memory cache if it exists, and return the latest
	// version that is less than or equal to the requested timestamp.
	if rangeVersions, ok := drm.ranges[rangeNumber]; ok {
		versions := make([]int64, 0, len(rangeVersions))

		for version := range rangeVersions {
			versions = append(versions, version)
		}

		slices.Sort(versions)

		for i := len(versions) - 1; i >= 0; i-- {
			rangeVersion := versions[i]

			if rangeVersion > timestamp {
				continue
			}

			if r, ok := rangeVersions[rangeVersion]; ok {
				return r, nil
			}
		}
	}

	// Get the latest version of the range from the index.
	found, rangeVersion, err := drm.index.Get(rangeNumber)

	if err != nil {
		return nil, err
	}

	// // If the range version is greater than the requested timestamp, return an error.
	// if timestamp != 0 && rangeVersion > timestamp {
	// 	log.Println("requested timestamp:", timestamp, "range version:", rangeVersion)
	// 	return nil, errors.New("range version is greater than requested timestamp")
	// }

	var r *Range

	if !found || rangeVersion == 0 {
		// Open the range.
		r, err = NewRange(
			drm.dfs.databaseId,
			drm.dfs.branchId,
			drm.dfs.tieredFS,
			rangeNumber,
			drm.dfs.pageSize,
			timestamp,
		)

		if err != nil {
			return nil, err
		}

		// Update the range index with the latest version.
		err = drm.index.Set(rangeNumber, timestamp)
	} else {
		r, err = NewRange(
			drm.dfs.databaseId,
			drm.dfs.branchId,
			drm.dfs.tieredFS,
			rangeNumber,
			drm.dfs.pageSize,
			rangeVersion,
		)
	}

	if err != nil {
		return nil, err
	}

	if _, ok := drm.ranges[rangeNumber]; !ok {
		drm.ranges[rangeNumber] = make(map[int64]*Range)
	}

	drm.ranges[rangeNumber][r.Timestamp] = r

	return r, nil
}

// GetOldestTimestamp returns the oldest timestamp that is still in use.
func (drm *DataRangeManager) GetOldestTimestamp() int64 {
	drm.mutex.RLock()
	defer drm.mutex.RUnlock()

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
func (drm *DataRangeManager) Remove(rangeNumber int64, timestamp int64) error {
	drm.mutex.Lock()
	defer drm.mutex.Unlock()

	if rangeVersions, ok := drm.ranges[rangeNumber]; ok {
		if _, ok := rangeVersions[timestamp]; !ok {
			return errors.New("range not found")
		}

		delete(rangeVersions, timestamp)

		// If no more versions exist for this range, remove the range entirely
		if len(rangeVersions) == 0 {
			delete(drm.ranges, rangeNumber)
		}
	}

	err := drm.index.Set(rangeNumber, 0)

	if err != nil {
		return err
	}

	return nil
}

// TODO: Need to check range logs to see if there are any ranges that have been marked for deletion
// RunGarbageCollection removes all range files that are older than the oldest timestamp in use.
func (drm *DataRangeManager) RunGarbageCollection() error {
	drm.mutex.Lock()
	defer drm.mutex.Unlock()

	// Find the oldest timestamp without calling GetOldestTimestamp() to avoid deadlock
	var oldestTimestamp int64
	first := true

	if len(drm.rangeUsage) == 0 {
		return nil // No active timestamps, nothing to clean up
	}

	for timestamp := range drm.rangeUsage {
		if first || timestamp < oldestTimestamp {
			oldestTimestamp = timestamp
			first = false
		}
	}

	var filesToRemove []string

	for rangeNumber, rangeVersions := range drm.ranges {
		for timestamp, r := range rangeVersions {
			if timestamp < oldestTimestamp {
				// Close and delete the range file
				if r != nil {
					err := r.Close()
					if err != nil {
						log.Printf("Error closing range file during garbage collection: %v", err)
					}

					err = r.Delete()
					if err != nil {
						log.Printf("Error deleting range file during garbage collection: %v", err)
					}

					filesToRemove = append(filesToRemove, "range file")
				}

				delete(rangeVersions, timestamp)
			}
		}

		// Remove empty range entries
		if len(rangeVersions) == 0 {
			delete(drm.ranges, rangeNumber)
		}
	}

	log.Printf("Garbage collected %d range files", len(filesToRemove))

	return nil
}
