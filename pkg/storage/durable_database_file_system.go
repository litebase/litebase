package storage

import (
	"bytes"
	"io"
	"log"
	"log/slog"
	"sync"
	"time"

	"github.com/litebase/litebase/pkg/file"

	internalStorage "github.com/litebase/litebase/internal/storage"
)

// OPTIMIZE: Use an LRU cache for page data
// TODO: Do we need to limit the number of open ranges?
type DurableDatabaseFileSystem struct {
	buffers      *sync.Pool
	branchId     string
	databaseId   string
	tieredFS     *FileSystem
	RangeManager *DataRangeManager
	metadata     *DatabaseMetadata
	mutex        *sync.RWMutex
	path         string
	PageLogger   *PageLogger
	pageSize     int64
	writeHook    func(offset int64, data []byte)
}

func NewDurableDatabaseFileSystem(
	tieredFS *FileSystem,
	networkFS *FileSystem,
	pageLogger *PageLogger,
	path, databaseId, branchId string,
	pageSize int64,
) *DurableDatabaseFileSystem {
	dfs := &DurableDatabaseFileSystem{
		branchId: branchId,
		buffers: &sync.Pool{
			New: func() any {
				return bytes.NewBuffer(make([]byte, pageSize))
			},
		},
		databaseId: databaseId,
		tieredFS:   tieredFS,
		mutex:      &sync.RWMutex{},
		PageLogger: pageLogger,
		path:       path,
		pageSize:   pageSize,
	}

	dfs.RangeManager = NewDataRangeManager(dfs)

	err := dfs.init()

	if err != nil {
		log.Println("Error initializing database file system", err)
		return nil
	}

	return dfs
}

// Acquire marks a range as being used at the specified timestamp.
func (dfs *DurableDatabaseFileSystem) Acquire(timestamp int64) {
	dfs.RangeManager.Acquire(timestamp)
}

// Run compaction on the page logger of the database file system.
func (dfs *DurableDatabaseFileSystem) Compact() error {
	dfs.mutex.Lock()
	defer dfs.mutex.Unlock()

	err := dfs.PageLogger.Compact(dfs)

	if err != nil {
		slog.Error("Error compacting database file system", "error", err)
	}

	return dfs.RangeManager.RunGarbageCollection()
}

// CompactionBarrier runs the given function while preventing compaction from
// occurring during its execution.
func (dfs *DurableDatabaseFileSystem) CompactionBarrier(fn func() error) error {
	return dfs.PageLogger.CompactionBarrier(fn)
}

// Compact data to the latest version of a range by creating a copy of the
// latest range so the caller can make modifications with an atomic operation.
func (dfs *DurableDatabaseFileSystem) compactToRange(rangeNumber int64, fn func(newRange *Range) error) error {
	found, rangeTimestamp, err := dfs.RangeManager.Index.Get(rangeNumber)

	if err != nil {
		return err
	}

	if !found {
		panic("Range not found in index")
	}

	newTimestamp := rangeTimestamp + 1

	// Create a new range for this batch of pages
	_, err = dfs.RangeManager.CopyRange(rangeNumber, newTimestamp, fn)

	if err != nil {
		return err
	}

	return nil
}

func (dfs *DurableDatabaseFileSystem) Delete(path string) error {
	// No-op since pages are stored in separate files and we don't need to
	// delete the database "file".

	return nil
}

func (dfs *DurableDatabaseFileSystem) Exists() bool {
	_, err := dfs.tieredFS.Stat(dfs.path)

	return err == nil
}

// Return the FileSystem that this DurableDatabaseFileSystem is using.
func (dfs *DurableDatabaseFileSystem) FileSystem() *FileSystem {
	return dfs.tieredFS
}

// ForceCompact forces the page logger to compact the database file system.
func (dfs *DurableDatabaseFileSystem) ForceCompact() error {
	return dfs.PageLogger.ForceCompact(dfs)
}

// GetRangeFile returns the range file for the given range number.
func (dfs *DurableDatabaseFileSystem) GetRangeFile(rangeNumber int64) (*Range, error) {
	r, err := dfs.RangeManager.Get(rangeNumber, time.Now().UTC().UnixNano())

	if err != nil {
		return nil, err
	}

	return r, nil
}

// Initialize the database file system by loading the metadata and the first
// range file.
func (dfs *DurableDatabaseFileSystem) init() error {
	wg := sync.WaitGroup{}

	var initErrors []error

	// Load the metadata for the database
	wg.Add(1)
	go func() {
		defer wg.Done()
		metadata, err := NewDatabaseMetadata(dfs, dfs.databaseId, dfs.branchId)

		if err != nil {
			log.Println("Error creating database metadata", err)

			initErrors = append(initErrors, err)
			return
		}

		dfs.metadata = metadata
	}()

	// Load the first range file
	wg.Add(1)
	go func() {
		defer wg.Done()
		// This is an optimization for the first range file, so using timestamp 1
		// to avoid conflicts with restored ranges that have higher timestamps
		_, err := dfs.RangeManager.Get(1, 1)

		if err != nil {
			log.Println("Error creating range file", err)
			initErrors = append(initErrors, err)
			return
		}
	}()

	// Wait for all goroutines to finish
	wg.Wait()

	if len(initErrors) > 0 {
		for _, err := range initErrors {
			log.Println("Error initializing database file system", err)
		}

		return initErrors[0]
	}

	return nil
}

func (dfs *DurableDatabaseFileSystem) Metadata() *DatabaseMetadata {
	return dfs.metadata
}

func (dfs *DurableDatabaseFileSystem) Open(path string) (internalStorage.File, error) {
	// No-op since pages are stored in separate files and we don't need to open
	// the database "file".
	return nil, nil
}

func (dfs *DurableDatabaseFileSystem) PageSize() int64 {
	return dfs.pageSize
}

func (dfs *DurableDatabaseFileSystem) Path() string {
	return dfs.path
}

func (dfs *DurableDatabaseFileSystem) ReadAt(walTimestamp, transactionalTimestamp int64, data []byte, offset, length int64) (int, error) {
	dfs.mutex.RLock()
	defer dfs.mutex.RUnlock()

	pageNumber := file.PageNumber(offset, dfs.pageSize)

	found, _, err := dfs.PageLogger.Read(pageNumber, walTimestamp, data)

	if err != nil {
		log.Println("Error reading page", pageNumber, err)
		return 0, err
	}

	if found {
		return len(data), nil
	}

	// Get the range file for the page using the range manager
	rangeFile, err := dfs.RangeManager.Get(
		file.PageRange(pageNumber, RangeMaxPages),
		transactionalTimestamp,
	)

	if err != nil {
		log.Println("Error getting range file", err)
		return 0, err
	}

	n, err := rangeFile.ReadAt(pageNumber, data)

	if err != nil {
		if err != io.EOF {
			log.Println("Error reading page", pageNumber, err)
			return 0, err
		}
	}

	return n, err
}

// Release marks a range as no longer being used at the specified timestamp.
func (dfs *DurableDatabaseFileSystem) Release(timestamp int64) {
	dfs.RangeManager.Release(timestamp)
}

func (dfs *DurableDatabaseFileSystem) SetWriteHook(hook func(offset int64, data []byte)) *DurableDatabaseFileSystem {
	dfs.writeHook = hook

	return dfs
}

func (dfs *DurableDatabaseFileSystem) Size() (int64, error) {
	return dfs.metadata.FileSize(), nil
}

func (dfs *DurableDatabaseFileSystem) Shutdown() error {
	dfs.mutex.Lock()
	defer dfs.mutex.Unlock()

	err := dfs.RangeManager.Close()

	if err != nil {
		slog.Error("Error closing range manager", "error", err)
	}

	err = dfs.metadata.Close()

	if err != nil {
		slog.Warn("Error closing metadata", "error", err)
	}

	return nil
}

// Truncate or remove the ranges based on the number of pages that need to be
// removed. Each range can hold RangeMaxPages pages. This routine is typically
// called when the database is being vacuumed so we can remove the pages that are
// no longer needed.

// The number of pages that need to be removed is calculated by the difference
// between the current size of the database and the new size of the database.
// Where there is a remainder, we need to remove the last range file and truncate
// the range file that contains the last page that needs to be removed.
func (dfs *DurableDatabaseFileSystem) Truncate(size int64) error {
	dfs.mutex.Lock()
	defer dfs.mutex.Unlock()

	// Force compaction before truncating
	if err := dfs.ForceCompact(); err != nil {
		slog.Error("Error forcing compaction before truncating", "error", err)
		return err
	}

	currentSize := dfs.metadata.FileSize()

	if size >= currentSize {
		return nil
	}

	bytesToRemove := currentSize - size

	startingPage := size/dfs.pageSize + 1
	endingPage := currentSize / dfs.pageSize
	startingRange := file.PageRange(startingPage, RangeMaxPages)
	endingRange := file.PageRange(endingPage, RangeMaxPages)

	// Open ranges from end to start and continue until the bytesToRemove is 0
	for rangeNumber := endingRange; rangeNumber >= startingRange; rangeNumber-- {
		r, err := dfs.RangeManager.Get(rangeNumber, time.Now().UTC().UnixNano())

		if err != nil {
			slog.Error("Error getting range file", "error", err)
			return err
		}

		rangeSize, err := r.Size()

		if err != nil {
			slog.Error("Error getting range size", "error", err)
			return err
		}

		if rangeSize <= bytesToRemove {
			rangePageCount := r.PageCount()

			err := r.Delete()

			if err != nil {
				slog.Error("Error removing range", "error", err)
				return err
			}

			// Remove the range from the range manager
			err = dfs.RangeManager.Remove(rangeNumber, r.Timestamp)

			if err != nil {
				slog.Error("Error removing range from range manager", "error", err)
				return err
			}

			err = dfs.metadata.SetPageCount(dfs.metadata.PageCount - rangePageCount)

			if err != nil {
				slog.Error("Error setting page count", "error", err)
				return err
			}

			bytesToRemove -= rangeSize
		} else {
			err := r.Truncate(rangeSize - bytesToRemove)

			if err != nil {
				slog.Error("Error truncating range", "error", err)

				return err
			}

			bytesToRemove = 0

			pageCount := r.PageCount()

			err = dfs.metadata.SetPageCount(pageCount)

			if err != nil {
				slog.Error("Error setting page count", "error", err)
				return err
			}
		}

		if bytesToRemove == 0 {
			break
		}
	}

	return nil
}

// Write to the DurableDatabaseFileSystem at the specified offset at a timestamp.
func (dfs *DurableDatabaseFileSystem) WriteAt(walTimestamp, transactionalTimestamp int64, data []byte, offset int64) (n int, err error) {
	dfs.mutex.Lock()
	defer dfs.mutex.Unlock()

	pageNumber := file.PageNumber(offset, dfs.pageSize)

	if dfs.writeHook != nil {
		buffer := dfs.buffers.Get().(*bytes.Buffer)
		defer dfs.buffers.Put(buffer)

		buffer.Reset()

		currentPageData := buffer.Bytes()[:len(data)]

		found, _, err := dfs.PageLogger.Read(pageNumber, walTimestamp, currentPageData)

		if err != nil {
			return 0, err
		}

		if !found {
			rangeFile, err := dfs.RangeManager.Get(file.PageRange(pageNumber, RangeMaxPages), transactionalTimestamp)

			if err != nil {
				log.Println("Error getting range file", err)
				return 0, err
			}

			// Get the current version of the page
			_, err = rangeFile.ReadAt(pageNumber, currentPageData)

			if err != nil {
				log.Println("Error reading page for write hook", err)

				return 0, err
			}
		}

		// Call the write hook
		dfs.writeHook(offset, currentPageData)
	}

	n, err = dfs.PageLogger.Write(pageNumber, walTimestamp, data)

	if err != nil {
		return 0, err
	}

	// Get the range file for the page
	// rangeFile, err := dfs.GetRangeFile(file.PageRange(pageNumber, RangeMaxPages))

	// if err != nil {
	// 	log.Println("Error getting range file", err)
	// 	return 0, err
	// }

	// n, err = rangeFile.WriteAt(pageNumber, data)

	// if err != nil {
	// 	log.Println("Error writing page", pageNumber, err)
	// 	return 0, err
	// }

	if dfs.metadata.PageCount < pageNumber {
		err := dfs.metadata.SetPageCount(pageNumber)

		if err != nil {
			slog.Error("Error setting page count", "error", err)
		}
	}

	return n, nil
}

func (dfs *DurableDatabaseFileSystem) WriteHook(offset int64, data []byte) {
	dfs.writeHook(offset, data)
}

func (dfs *DurableDatabaseFileSystem) WriteToRange(pageNumber int64, data []byte) error {
	dfs.mutex.Lock()
	defer dfs.mutex.Unlock()

	rangeFile, err := dfs.RangeManager.Get(file.PageRange(pageNumber, RangeMaxPages), time.Now().UTC().UnixNano())

	if err != nil {
		return err
	}

	_, err = rangeFile.WriteAt(pageNumber, data)

	if err != nil {
		return err
	}

	return nil
}

func (dfs *DurableDatabaseFileSystem) WriteWithoutWriteHook(fn func() (int, error)) (int, error) {
	dfs.mutex.Lock()
	writeHook := dfs.writeHook
	dfs.writeHook = nil
	dfs.mutex.Unlock()

	n, err := fn()

	dfs.mutex.Lock()
	dfs.writeHook = writeHook
	dfs.mutex.Unlock()

	return n, err
}
