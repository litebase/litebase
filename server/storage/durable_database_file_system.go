package storage

import (
	"fmt"
	"io"
	internalStorage "litebase/internal/storage"
	"litebase/server/file"
	"log"
	"os"
	"strings"
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
	timestamp    int64
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

func (dfs *DurableDatabaseFileSystem) getRangeFile(rangeNumber int64) (*DataRange, error) {
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
	path = fmt.Sprintf("%s/%s", dfs.path, strings.ReplaceAll(path, ".db", ""))

	err := dfs.fileSystem.MkdirAll(path, 0755)

	if err != nil {
		return nil, err
	}

	return nil, nil
}

func (dfs *DurableDatabaseFileSystem) PageSize() int64 {
	return dfs.pageSize
}

func (dfs *DurableDatabaseFileSystem) Path() string {
	return dfs.path
}

func (dfs *DurableDatabaseFileSystem) ReadAt(path string, data []byte, offset, length int64) (int, error) {
	dfs.mutex.RLock()
	defer dfs.mutex.RUnlock()

	pageNumber := file.PageNumber(offset, dfs.pageSize)
	// log.Println("RANGE", file.PageRange(pageNumber, DataRangeMaxPages))
	// Get the range file for the page
	rangeFile, err := dfs.getRangeFile(file.PageRange(pageNumber, DataRangeMaxPages))

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

func (dfs *DurableDatabaseFileSystem) SetTransactionTimestamp(timestamp int64) {
	dfs.timestamp = timestamp
}

// TODO: this should use the metadata file to get the size
func (dfs *DurableDatabaseFileSystem) Size(path string) (int64, error) {
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

func (dfs *DurableDatabaseFileSystem) TransactionTimestamp() int64 {
	return dfs.timestamp
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
func (dfs *DurableDatabaseFileSystem) Truncate(path string, size int64) error {
	dfs.mutex.Lock()
	defer dfs.mutex.Unlock()

	currentSize := dfs.metadata.FileSize()

	if size >= currentSize {
		return nil
	}

	bytesToRemove := size
	startingPage := size/dfs.pageSize + 1
	endingPage := currentSize / dfs.pageSize
	startingRange := file.PageRange(startingPage, DataRangeMaxPages)
	endingRange := file.PageRange(endingPage, DataRangeMaxPages)

	// Open ranges from end to start and continue until the bytesToRemove is 0
	for rangeNumber := endingRange; rangeNumber >= startingRange; rangeNumber-- {
		dataRange, err := dfs.getRangeFile(rangeNumber)

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
			err := dataRange.Delete()

			if err != nil {
				log.Println("Error removing range", err)
				return err
			}

			// Remove the range from the map
			dfs.mutex.Lock()
			delete(dfs.dataRanges, rangeNumber)
			dfs.mutex.Unlock()

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

func (dfs *DurableDatabaseFileSystem) WalPath(path string) string {
	return ""
}

func (dfs *DurableDatabaseFileSystem) WithTransactionTimestamp(timestamp int64) *DurableDatabaseFileSystem {
	dfs.timestamp = timestamp

	return dfs
}

func (dfs *DurableDatabaseFileSystem) WithWriteHook(hook func(offset int64, data []byte)) *DurableDatabaseFileSystem {
	dfs.writeHook = hook

	return dfs
}

func (dfs *DurableDatabaseFileSystem) WriteAt(path string, data []byte, offset int64) (n int, err error) {
	dfs.mutex.Lock()
	defer dfs.mutex.Unlock()

	pageNumber := file.PageNumber(offset, dfs.pageSize)
	rangeFile, err := dfs.getRangeFile(file.PageRange(pageNumber, DataRangeMaxPages))

	if err != nil {
		log.Println("Error getting range file", err)
		return 0, err
	}

	n, err = rangeFile.WriteAt(data, pageNumber)
	// n, err = rangeFile.WriteAt(data, pageNumber, dfs.timestamp)

	if err != nil {
		log.Println("Error writing page", pageNumber, err)
		return 0, err
	}

	if dfs.writeHook != nil {
		dfs.writeHook(offset, data)
	}

	if dfs.metadata.PageCount < pageNumber {
		dfs.metadata.SetPageCount(pageNumber)
	}

	return n, nil
}

func (dfs *DurableDatabaseFileSystem) WriteHook(offset int64, data []byte) {
	dfs.writeHook(offset, data)
}
