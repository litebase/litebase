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

type LocalDatabaseFileSystem struct {
	dataRanges map[int64]*DataRange
	fileSystem *LocalFileSystemDriver
	hasPageOne bool
	mutex      *sync.RWMutex
	path       string
	pageSize   int64
	size       int64
	timestamp  int64
	writeHook  func(offset int64, data []byte)
}

func NewLocalDatabaseFileSystem(path, databaseUuid, branchUuid string, pageSize int64) *LocalDatabaseFileSystem {
	fs := NewLocalFileSystemDriver()

	// Check if the the directory exists
	if _, err := fs.Stat(path); err != nil {
		if os.IsNotExist(err) {
			// Create the directory
			if err := fs.MkdirAll(path, 0755); err != nil {
				log.Fatalln("Error creating temp file system directory", err)
			}
		} else {
			log.Fatalln("Error checking temp file system directory", err)
		}
	}

	return &LocalDatabaseFileSystem{
		dataRanges: make(map[int64]*DataRange),
		fileSystem: fs,
		mutex:      &sync.RWMutex{},
		path:       path,
		pageSize:   pageSize,
	}
}

func (lfs *LocalDatabaseFileSystem) Close(path string) error {
	lfs.mutex.Lock()
	defer lfs.mutex.Unlock()

	for key, dataRange := range lfs.dataRanges {
		dataRange.Close()
		delete(lfs.dataRanges, key)
	}

	return nil
}

func (lfs *LocalDatabaseFileSystem) Delete(path string) error {
	// No-op since pages are stored in separate files and we don't need to
	// delete the database "file".

	return nil
}

func (lfs *LocalDatabaseFileSystem) Exists() bool {
	_, err := lfs.fileSystem.Stat(lfs.path)

	return err == nil
}

func (lfs *LocalDatabaseFileSystem) getRangeFile(path string, rangeNumber int64) (*DataRange, error) {
	if dataRange, ok := lfs.dataRanges[rangeNumber]; ok {
		return dataRange, nil
	}

	dataRange, err := NewDataRange(lfs, path, rangeNumber)

	if err != nil {
		log.Println("Error creating data range", err)
		return nil, err
	}

	lfs.dataRanges[rangeNumber] = dataRange

	return dataRange, nil
}

func (lfs *LocalDatabaseFileSystem) Open(path string) (internalStorage.File, error) {
	path = fmt.Sprintf("%s/%s", lfs.path, strings.ReplaceAll(path, ".db", ""))

	err := lfs.fileSystem.MkdirAll(path, 0755)

	if err != nil {
		return nil, err
	}

	return nil, nil
}

func (lfs *LocalDatabaseFileSystem) Path() string {
	return lfs.path
}

func (lfs *LocalDatabaseFileSystem) ReadAt(path string, data []byte, offset, length int64) (int, error) {
	lfs.mutex.RLock()
	defer lfs.mutex.RUnlock()

	pageNumber := file.PageNumber(offset, lfs.pageSize)

	// Get the range file for the page
	rangeFile, err := lfs.getRangeFile(path, file.PageRange(pageNumber, DataRangeMaxPages))

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

	if pageNumber == 1 && n > 0 {
		lfs.hasPageOne = true
	}

	return n, nil
}

func (lfs *LocalDatabaseFileSystem) SetTransactionTimestamp(timestamp int64) {
	lfs.timestamp = timestamp
}

// TODO: this should use the metadata file to get the size
func (lfs *LocalDatabaseFileSystem) Size(path string) (int64, error) {
	if lfs.hasPageOne {
		lfs.size = lfs.pageSize * 4294967294
	}

	return lfs.size, nil
}

func (lfs *LocalDatabaseFileSystem) TransactionTimestamp() int64 {
	return lfs.timestamp
}

func (lfs *LocalDatabaseFileSystem) Truncate(path string, size int64) error {
	// No-op since pages are stored in separate files and we don't need to

	return nil
}

func (lfs *LocalDatabaseFileSystem) WalPath(path string) string {
	return ""
}

func (lfs *LocalDatabaseFileSystem) WithTransactionTimestamp(timestamp int64) DatabaseFileSystem {
	lfs.timestamp = timestamp

	return lfs
}

func (lfs *LocalDatabaseFileSystem) WithWriteHook(hook func(offset int64, data []byte)) DatabaseFileSystem {
	lfs.writeHook = hook

	return lfs
}

func (lfs *LocalDatabaseFileSystem) WriteAt(path string, data []byte, offset int64) (n int, err error) {
	lfs.mutex.Lock()
	defer lfs.mutex.Unlock()

	pageNumber := file.PageNumber(offset, lfs.pageSize)

	rangeFile, err := lfs.getRangeFile(path, file.PageRange(pageNumber, DataRangeMaxPages))

	if err != nil {
		log.Println("Error getting range file", err)
		return 0, err
	}

	n, err = rangeFile.WriteAt(data, pageNumber)
	// n, err = rangeFile.WriteAt(data, pageNumber, lfs.timestamp)

	if err != nil {
		log.Println("Error writing page", pageNumber, err)
		return 0, err
	}

	if lfs.writeHook != nil {
		lfs.writeHook(offset, data)
	}

	if pageNumber == 1 {
		lfs.hasPageOne = true
	}

	return n, nil
}

func (lfs *LocalDatabaseFileSystem) WriteHook(offset int64, data []byte) {
	lfs.writeHook(offset, data)
}
