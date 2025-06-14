package storage

import (
	"bytes"
	"io"
	"log"
	"sync"

	"github.com/litebase/litebase/server/file"

	internalStorage "github.com/litebase/litebase/internal/storage"
)

// OPTIMIZE: Use an LRU cache for page data
// TODO: Do we need to limit the number of open ranges?
type DurableDatabaseFileSystem struct {
	buffers    *sync.Pool
	branchId   string
	databaseId string
	tieredFS   *FileSystem
	ranges     map[int64]*Range
	metadata   *DatabaseMetadata
	mutex      *sync.RWMutex
	path       string
	PageLogger *PageLogger
	pageSize   int64
	writeHook  func(offset int64, data []byte)
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
		ranges:     make(map[int64]*Range),
		tieredFS:   tieredFS,
		mutex:      &sync.RWMutex{},
		PageLogger: pageLogger,
		path:       path,
		pageSize:   pageSize,
	}

	err := dfs.init()

	if err != nil {
		log.Println("Error initializing database file system", err)
		return nil
	}

	return dfs
}

func (dfs *DurableDatabaseFileSystem) Compact() error {
	return dfs.PageLogger.Compact(dfs)
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

func (dfs *DurableDatabaseFileSystem) FileSystem() *FileSystem {
	return dfs.tieredFS
}

func (dfs *DurableDatabaseFileSystem) GetRangeFile(rangeNumber int64) (*Range, error) {
	if r, ok := dfs.ranges[rangeNumber]; ok {
		return r, nil
	}

	path := file.GetDatabaseFileDir(dfs.databaseId, dfs.branchId)

	r, err := NewRange(
		dfs.databaseId,
		dfs.branchId,
		dfs.tieredFS,
		path,
		rangeNumber,
		dfs.pageSize,
	)

	if err != nil {
		log.Println("Error creating range", err)
		return nil, err
	}

	dfs.ranges[rangeNumber] = r

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
		_, err := dfs.GetRangeFile(1)

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

func (dfs *DurableDatabaseFileSystem) ReadAt(timestamp int64, data []byte, offset, length int64) (int, error) {
	dfs.mutex.RLock()
	defer dfs.mutex.RUnlock()

	pageNumber := file.PageNumber(offset, dfs.pageSize)

	found, _, err := dfs.PageLogger.Read(pageNumber, timestamp, data)

	if err != nil {
		log.Println("Error reading page", pageNumber, err)
		return 0, err
	}

	if found {
		return len(data), nil
	}

	// Get the range file for the page
	rangeFile, err := dfs.GetRangeFile(file.PageRange(pageNumber, RangeMaxPages))

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

	for key, r := range dfs.ranges {
		r.Close()
		delete(dfs.ranges, key)
	}

	dfs.metadata.Close()

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
		r, err := dfs.GetRangeFile(rangeNumber)

		if err != nil {
			log.Println("Error getting range file", err)
			return err
		}

		rangeSize, err := r.Size()

		if err != nil {
			log.Println("Error getting range size", err)
			return err
		}

		if rangeSize <= bytesToRemove {
			rangePageCount := r.PageCount()

			err := r.Delete()

			if err != nil {
				log.Println("Error removing range", err)
				return err
			}

			// Remove the range from the map
			delete(dfs.ranges, rangeNumber)

			dfs.metadata.SetPageCount(dfs.metadata.PageCount - rangePageCount)

			bytesToRemove -= rangeSize
		} else {
			err := r.Truncate(rangeSize - bytesToRemove)

			if err != nil {
				log.Println("Error truncating range", err)

				return err
			}

			bytesToRemove = 0

			pageCount := r.PageCount()

			dfs.metadata.SetPageCount(pageCount)
		}

		if bytesToRemove == 0 {
			break
		}
	}

	return nil
}

// Write to the DurableDatabaseFileSystem at the specified offset at a timestamp.
func (dfs *DurableDatabaseFileSystem) WriteAt(timestamp int64, data []byte, offset int64) (n int, err error) {
	dfs.mutex.Lock()
	defer dfs.mutex.Unlock()

	pageNumber := file.PageNumber(offset, dfs.pageSize)

	if dfs.writeHook != nil {
		buffer := dfs.buffers.Get().(*bytes.Buffer)
		defer dfs.buffers.Put(buffer)

		buffer.Reset()

		currentPageData := buffer.Bytes()[:len(data)]

		found, _, err := dfs.PageLogger.Read(pageNumber, timestamp, currentPageData)

		if err != nil {
			return 0, err
		}

		if !found {
			rangeFile, err := dfs.GetRangeFile(file.PageRange(pageNumber, RangeMaxPages))

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

	n, err = dfs.PageLogger.Write(pageNumber, timestamp, data)

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
		dfs.metadata.SetPageCount(pageNumber)
	}

	return n, nil
}

func (dfs *DurableDatabaseFileSystem) WriteHook(offset int64, data []byte) {
	dfs.writeHook(offset, data)
}

func (dfs *DurableDatabaseFileSystem) WriteToRange(pageNumber int64, data []byte) error {
	dfs.mutex.Lock()
	defer dfs.mutex.Unlock()

	rangeFile, err := dfs.GetRangeFile(file.PageRange(pageNumber, RangeMaxPages))

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
