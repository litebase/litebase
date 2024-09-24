package storage

import (
	"io"
	internalStorage "litebase/internal/storage"
	"litebase/server/file"
	"log"
	"os"
	"sync"
)

type DurableDatabaseFileSystem struct {
	branchUuid   string
	databaseUuid string
	dataRanges   map[int64]*DataRange
	fileSystem   *FileSystem
	metadata     *DatabaseMetadata
	mutex        *sync.RWMutex
	path         string
	pageSize     int64
	writeHook    func(offset int64, data []byte)
}

func NewDurableDatabaseFileSystem(fs *FileSystem, path, databaseUuid, branchUuid string, pageSize int64) *DurableDatabaseFileSystem {
	// Check if the the directory exists
	if _, err := fs.Stat(path); err != nil {
		if os.IsNotExist(err) {
			// Create the directory
			if err := fs.MkdirAll(path, 0755); err != nil {
				log.Fatalln("Error creating temp file system directory", err)
			}
		}
	}

	dfs := &DurableDatabaseFileSystem{
		branchUuid:   branchUuid,
		databaseUuid: databaseUuid,
		dataRanges:   make(map[int64]*DataRange),
		fileSystem:   fs,
		mutex:        &sync.RWMutex{},
		path:         path,
		pageSize:     pageSize,
	}

	metadata, err := NewDatabaseMetadata(dfs, databaseUuid, branchUuid)

	if err != nil {
		log.Println("Error creating database metadata", err)

		return nil
	}

	dfs.metadata = metadata

	return dfs
}

func (dfs *DurableDatabaseFileSystem) Delete(path string) error {
	// No-op since pages are stored in separate files and we don't need to
	// delete the database "file".

	return nil
}

func (dfs *DurableDatabaseFileSystem) Exists() bool {
	_, err := dfs.fileSystem.Stat(dfs.path)

	return err == nil
}

func (dfs *DurableDatabaseFileSystem) FileSystem() *FileSystem {
	return dfs.fileSystem
}

func (dfs *DurableDatabaseFileSystem) GetRangeFile(rangeNumber int64) (*DataRange, error) {
	if dataRange, ok := dfs.dataRanges[rangeNumber]; ok {
		return dataRange, nil
	}

	path := file.GetDatabaseFileDir(dfs.databaseUuid, dfs.branchUuid)

	dataRange, err := NewDataRange(dfs.fileSystem, path, rangeNumber, dfs.pageSize)

	if err != nil {
		log.Println("Error creating data range", err)
		return nil, err
	}

	dfs.dataRanges[rangeNumber] = dataRange

	return dataRange, nil
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

func (dfs *DurableDatabaseFileSystem) ReadAt(data []byte, offset, length int64) (int, error) {
	dfs.mutex.RLock()
	defer dfs.mutex.RUnlock()

	pageNumber := file.PageNumber(offset, dfs.pageSize)
	// log.Println("RANGE", file.PageRange(pageNumber, DataRangeMaxPages))
	// Get the range file for the page
	rangeFile, err := dfs.GetRangeFile(file.PageRange(pageNumber, DataRangeMaxPages))

	if err != nil {
		log.Println("Error getting range file", err)
		return 0, err
	}

	n, err := rangeFile.ReadAt(data, pageNumber)

	if err != nil {
		if err != io.EOF {
			log.Println("Error reading page", pageNumber, err)
			return 0, err
		}
	}

	return n, nil
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

	for key, dataRange := range dfs.dataRanges {
		dataRange.Close()
		delete(dfs.dataRanges, key)
	}

	dfs.metadata.Close()

	return nil
}

/*
Truncate or remove the data ranges based on the number of pages that need to be
removed. Each range can hold DataRangeMaxPages pages. This routine is typically
called when the database is being vacuumed so we can remove the pages that are
no longer needed.

The number of pages that need to be removed is calculated by the difference
between the current size of the database and the new size of the database.
Where there is a remainder, we need to remove the last range file and truncate
the range file that contains the last page that needs to be removed.
*/
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
	startingRange := file.PageRange(startingPage, DataRangeMaxPages)
	endingRange := file.PageRange(endingPage, DataRangeMaxPages)

	// Open ranges from end to start and continue until the bytesToRemove is 0
	for rangeNumber := endingRange; rangeNumber >= startingRange; rangeNumber-- {
		dataRange, err := dfs.GetRangeFile(rangeNumber)

		if err != nil {
			log.Println("Error getting range file", err)
			return err
		}

		rangeSize, err := dataRange.Size()

		if err != nil {
			log.Println("Error getting range size", err)
			return err
		}

		if rangeSize <= bytesToRemove {
			dataRangePageCount := dataRange.PageCount()
			err := dataRange.Delete()

			if err != nil {
				log.Println("Error removing range", err)
				return err
			}

			// Remove the range from the map
			delete(dfs.dataRanges, rangeNumber)

			dfs.metadata.SetPageCount(dfs.metadata.PageCount - dataRangePageCount)

			bytesToRemove -= rangeSize
		} else {
			err := dataRange.Truncate(bytesToRemove)

			if err != nil {
				log.Println("Error truncating range", err)

				return err
			}

			bytesToRemove = 0
		}

		if bytesToRemove == 0 {
			break
		}
	}

	return nil
}

func (dfs *DurableDatabaseFileSystem) WriteAt(data []byte, offset int64) (n int, err error) {
	dfs.mutex.Lock()
	defer dfs.mutex.Unlock()

	pageNumber := file.PageNumber(offset, dfs.pageSize)
	rangeFile, err := dfs.GetRangeFile(file.PageRange(pageNumber, DataRangeMaxPages))

	if err != nil {
		log.Println("Error getting range file", err)
		return 0, err
	}

	if dfs.writeHook != nil {
		// Get the current version of the page
		currentPageData := make([]byte, dfs.pageSize)

		_, err := rangeFile.ReadAt(currentPageData, pageNumber)

		if err != nil {
			log.Println("Error reading page for write hook", err)

			return 0, err
		}

		// Call the write hook
		dfs.writeHook(offset, currentPageData)
	}

	n, err = rangeFile.WriteAt(data, pageNumber)

	if err != nil {
		log.Println("Error writing page", pageNumber, err)
		return 0, err
	}

	if dfs.metadata.PageCount < pageNumber {
		dfs.metadata.SetPageCount(pageNumber)
	}

	return n, nil
}

func (dfs *DurableDatabaseFileSystem) WriteHook(offset int64, data []byte) {
	dfs.writeHook(offset, data)
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
