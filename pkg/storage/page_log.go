package storage

import (
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/litebase/litebase/internal/storage"
	"github.com/litebase/litebase/pkg/cache"
)

const (
	PageSize = 4096
)

var (
	PageLogSyncThreshold = int64(1000) // Number of writes before forcing a sync
)

type PageLog struct {
	cache               *cache.LFUCache
	compactedAt         time.Time
	deleted             bool
	fileSystem          *FileSystem
	file                storage.File
	index               *PageLogIndex
	mutex               *sync.Mutex
	Path                string
	size                int64
	writtenAt           time.Time
	writesSinceLastSync int64
}

// Create a new page log instance.
func NewPageLog(fileSystem *FileSystem, path string) (*PageLog, error) {
	pl := &PageLog{
		cache:      cache.NewLFUCache(100),
		fileSystem: fileSystem,
		mutex:      &sync.Mutex{},
		Path:       path,
	}

	var pli *PageLogIndex
	var err error

	wg := sync.WaitGroup{}
	wg.Add(2)

	go func() {
		defer wg.Done()

		pli = NewPageLogIndex(fileSystem, fmt.Sprintf("%s_INDEX", path))
	}()

	go func() {
		defer wg.Done()

		err = pl.openFile()

	}()

	wg.Wait()

	if err != nil {
		return nil, err
	}

	pl.index = pli

	return pl, nil
}

// Add a new page entry to the page log. This writes the page data to the log
// file and updates the index. This function is thread-safe and will lock the
// page log during the write operation.
func (pl *PageLog) Append(page int64, version int64, value []byte) error {
	pl.mutex.Lock()
	defer pl.mutex.Unlock()

	if len(value) != PageSize {
		return errors.New("value size is not equal to the required page size")
	}

	// Get the offset where we plan to write
	offset, err := pl.File().Seek(0, io.SeekEnd)

	if err != nil {
		return err
	}

	// Write the data to the file first
	bytesWritten, err := pl.File().Write(value)

	if err != nil {
		return err
	}

	// Ensure the entire page was written
	if bytesWritten != PageSize {
		return fmt.Errorf("incomplete write: expected %d bytes, wrote %d bytes", PageSize, bytesWritten)
	}

	if pl.shouldSync() {
		err = pl.sync()

		if err != nil {
			slog.Warn("Error syncing page log", "error", err)
		}
	}

	pl.size += int64(bytesWritten)
	pl.writesSinceLastSync += 1
	pl.writtenAt = time.Now()

	// Only update the index after the data is safely written and synced
	err = pl.index.Put(PageNumber(page), PageVersion(version), offset, value)

	if err != nil {
		// Index update failed - this is a serious consistency issue
		// We should log this error and potentially mark the page log as corrupted
		slog.Error("Failed to update page log index after successful write",
			"error", err, "page", page, "version", version, "offset", offset)
		return fmt.Errorf("failed to update index after write: %w", err)
	}

	// Update cache only after successful index update
	if pl.cache != nil {
		err = pl.cache.Put(fmt.Sprintf("%d:%d", page, version), value)
		if err != nil {
			slog.Warn("Failed to cache page log entry", "error", err, "page", page, "version", version)
		}
	}

	return nil
}

// Execute the close logic without locking the mutex.
func (pl *PageLog) close() error {
	var fileErr, indexErr error

	if pl.file != nil {
		fileErr = pl.file.Close()
		pl.file = nil
	}

	if pl.index != nil {
		indexErr = pl.index.Close()
	}

	pl.cache = nil

	// Return the first error encountered
	if fileErr != nil {
		return fileErr
	}

	return indexErr
}

// Close the page log.
func (pl *PageLog) Close() error {
	pl.mutex.Lock()
	defer pl.mutex.Unlock()

	return pl.close()
}

// Compact the page log contents into the durable file system.
func (pl *PageLog) compact(durableFileSystem *DurableDatabaseFileSystem) error {
	// TODO: The page log needs to be durably marked as compacted to avoid
	// overwrites. This also will allow us to retry compaction if it fails due
	// to a crash or other error.
	if !pl.compactedAt.IsZero() {
		return nil
	}

	if pl.deleted {
		panic("PageLog is deleted")
	}

	// Get the latest version of each page in the log.
	latestVersions := pl.index.getLatestPageVersions()
	data := make([]byte, PageSize)

	for _, entry := range latestVersions {
		found, _, err := pl.Get(entry.PageNumber, entry.Version, data)

		if err != nil {
			return err
		}

		if found {
			err := durableFileSystem.WriteToRange(int64(entry.PageNumber), data)

			if err != nil {
				return err
			}
		}
	}

	return nil
}

// Close and delete the PageLog file.
func (pl *PageLog) Delete() error {
	pl.mutex.Lock()
	defer pl.mutex.Unlock()

	err := pl.close()

	if err != nil {
		return err
	}

	pl.deleted = true

	err = pl.index.Delete()

	if err != nil {
		return err
	}

	pl.index = nil

	return pl.fileSystem.Remove(pl.Path)
}

// Return the file of the PageLog.
func (pl *PageLog) File() storage.File {
	if pl.deleted {
		return nil
	}

	if pl.file == nil {
		err := pl.openFile()

		if err != nil {
			log.Println("Error opening page log:", err)
			return nil
		}
	}

	return pl.file
}

// Get a page from the PageLog by page number and version.
func (pl *PageLog) Get(page PageNumber, version PageVersion, data []byte) (bool, PageVersion, error) {
	pl.mutex.Lock()
	defer pl.mutex.Unlock()

	if pl.size == 0 {
		return false, 0, nil // Empty log
	}

	if pl.cache != nil {
		if cachedValue, found := pl.cache.Get(fmt.Sprintf("%d:%d", page, version)); found {
			copy(data, cachedValue.([]byte))
			return true, version, nil
		}
	}

	found, foundVersion, offset, err := pl.index.Find(page, version)

	if err != nil {
		return false, 0, err
	}

	if !found {
		return false, 0, nil
	}

	_, err = pl.File().Seek(offset, io.SeekStart)

	if err != nil {
		log.Println("Error seeking to offset", offset, err)
		return false, 0, err
	}

	// Read the data - if we get EOF, it means the index is inconsistent with the file
	bytesRead, err := pl.File().Read(data)

	if err != nil {
		log.Println("Error reading page data", err)
		return false, 0, err
	}

	// Ensure we read the complete page
	if bytesRead != PageSize {
		slog.Error("Incomplete page read from log",
			"page", page,
			"version", version,
			"offset", offset,
			"expected_bytes", PageSize,
			"actual_bytes", bytesRead)
		return false, 0, nil // Return not found for incomplete reads
	}

	return true, foundVersion, nil
}

func (pl *PageLog) openFile() error {
	var err error

tryOpen:
	pl.file, err = pl.fileSystem.OpenFile(pl.Path, os.O_RDWR|os.O_CREATE, 0600)

	if err != nil {
		if os.IsNotExist(err) {
			err = pl.fileSystem.MkdirAll(filepath.Dir(pl.Path), 0750)

			if err != nil {
				return err
			}

			goto tryOpen
		}

		return err
	}

	fileinfo, err := pl.file.Stat()

	if err != nil {
		pl.file.Close()
		pl.file = nil
		return fmt.Errorf("failed to stat page log file: %w", err)
	}

	pl.size = fileinfo.Size()

	return nil
}

// Determine if we should sync the page log after a write. This is based on the
// last write time and the current time of the number of writes that have been
// added to the log since the last sync. This is used to limit the number of
// syncs to disk to improve performance while still ensuring durability.
func (pl *PageLog) shouldSync() bool {
	// If the last write was more than 1 second ago, we should sync
	if !pl.writtenAt.IsZero() && time.Since(pl.writtenAt) > time.Second {
		return true
	}

	if pl.writesSinceLastSync > PageLogSyncThreshold {
		return true
	}

	return false
}

// Sync the page logger to ensure all data is flushed to disk.
func (pl *PageLog) sync() error {
	if pl.deleted {
		return errors.New("cannot sync a deleted page log")
	}

	err := pl.File().Sync()

	if err != nil {
		slog.Warn("Error syncing page log", "error", err)
	}

	err = pl.index.File().Sync()

	if err != nil {
		slog.Warn("Error syncing page logger index", "error", err)
	}

	pl.writesSinceLastSync = 0

	return err
}

// Sync the page log with a mutex.
func (pl *PageLog) Sync() error {
	pl.mutex.Lock()
	defer pl.mutex.Unlock()

	return pl.sync()
}

// Mark all pages of a specific version as tombstoned.
func (pl *PageLog) Tombstone(version PageVersion) error {
	pl.mutex.Lock()
	defer pl.mutex.Unlock()

	pages := pl.index.findPagesByVersion(version)

	for _, pageNumber := range pages {
		err := pl.index.Tombstone(PageNumber(pageNumber), PageVersion(version))

		if err != nil {
			return err
		}

		// Invalidate the cache entry for this tombstoned page
		if pl.cache != nil {
			pl.cache.Delete(fmt.Sprintf("%d:%d", pageNumber, version))
		}
	}

	return nil
}

// Validate checks the integrity of the page log by ensuring all index entries
// point to valid data in the file.
func (pl *PageLog) Validate() error {
	pl.mutex.Lock()
	defer pl.mutex.Unlock()

	if pl.index.Empty() {
		return nil // Empty index is valid
	}

	// Get file size
	stat, err := pl.File().Stat()
	if err != nil {
		return fmt.Errorf("failed to get file stat: %w", err)
	}

	fileSize := stat.Size()
	invalidEntries := 0

	// Get all entries from the index
	latestVersions := pl.index.getLatestPageVersions()

	for pageNumber, entry := range latestVersions {
		// Check if the offset is within file bounds
		if entry.Offset+PageSize > fileSize {
			slog.Warn("Page log index entry points beyond file size",
				"page", pageNumber,
				"version", entry.Version,
				"offset", entry.Offset,
				"file_size", fileSize,
				"required_size", entry.Offset+PageSize)
			invalidEntries++
			continue
		}

		// Try to read the data to ensure it's accessible
		data := make([]byte, PageSize)
		_, err := pl.File().ReadAt(data, entry.Offset)
		if err != nil {
			slog.Warn("Failed to read page log entry",
				"page", pageNumber,
				"version", entry.Version,
				"offset", entry.Offset,
				"error", err)
			invalidEntries++
		}
	}

	if invalidEntries > 0 {
		return fmt.Errorf("page log validation failed: %d invalid entries out of %d total",
			invalidEntries, len(latestVersions))
	}

	return nil
}
