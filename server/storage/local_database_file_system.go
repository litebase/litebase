package storage

import (
	"fmt"
	"io"
	internalStorage "litebase/internal/storage"
	"litebase/server/file"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
)

type LocalDatabaseFileSystem struct {
	files         map[string]internalStorage.File
	fileSystem    *LocalFileSystemDriver
	hasPageOne    bool
	mutex         *sync.RWMutex
	path          string
	pagesPerRange int64
	pageSize      int64
	size          int64
	writeHook     func(path string, offset int64, data []byte)
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
		files:         make(map[string]internalStorage.File),
		fileSystem:    fs,
		mutex:         &sync.RWMutex{},
		path:          path,
		pagesPerRange: 1000,
		pageSize:      pageSize,
	}
}

func (lfs *LocalDatabaseFileSystem) Close(path string) error {
	lfs.mutex.Lock()
	defer lfs.mutex.Unlock()

	file, ok := lfs.files[path]

	if !ok {
		return os.ErrNotExist
	}

	delete(lfs.files, path)

	return file.Close()
}

func (lfs *LocalDatabaseFileSystem) Delete(path string) error {
	lfs.mutex.Lock()
	defer lfs.mutex.Unlock()

	file, ok := lfs.files[path]

	if ok {
		delete(lfs.files, path)
		file.Close()
	}

	lfs.fileSystem.Remove(fmt.Sprintf("%s/%s", lfs.path, path))

	return nil
}

func (lfs *LocalDatabaseFileSystem) Exists() bool {
	_, err := lfs.fileSystem.Stat(lfs.path)

	return err == nil
}

func (lfs *LocalDatabaseFileSystem) getRangeFile(path string, pageNumber int64) (internalStorage.File, error) {
	directory := strings.ReplaceAll(path, ".db", "")
	rangeNumber := file.PageRange(pageNumber, lfs.pagesPerRange)

	var builder strings.Builder
	builder.Grow(len(lfs.path) + len(directory) + 12) // Preallocate memory
	builder.WriteString(lfs.path)
	builder.WriteString("/")
	builder.WriteString(directory)
	builder.WriteString("/")

	// Create a strings.Builder for efficient string concatenation
	var pageNumberBuilder strings.Builder
	pageNumberBuilder.Grow(10) // Preallocate memory for 10 characters

	// Convert rangeNumber to a zero-padded 10-digit string
	rangeStr := strconv.FormatInt(rangeNumber, 10)
	padding := 10 - len(rangeStr)
	for i := 0; i < padding; i++ {
		pageNumberBuilder.WriteByte('0')
	}
	pageNumberBuilder.WriteString(rangeStr)

	builder.WriteString(pageNumberBuilder.String())
	path = builder.String()

	if file, ok := lfs.files[path]; ok {
		return file, nil
	}

	file, err := lfs.fileSystem.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)

	if err != nil {
		log.Println("Error opening range file", err)
		return nil, err
	}

	lfs.files[path] = file

	return file, nil
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
	pageNumber := file.PageNumber(offset, lfs.pageSize)

	// Get the range file for the page
	rangeFile, err := lfs.getRangeFile(path, pageNumber)

	if err != nil {
		log.Println("Error getting range file", err)
		return 0, err
	}

	// lfs.mutex.Lock()
	// defer lfs.mutex.Unlock()

	n, err := rangeFile.ReadAt(data, file.PageRangeOffset(pageNumber, lfs.pagesPerRange, lfs.pageSize))

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
	// No-op
}

// TODO: this should use the metadata file to get the size
func (lfs *LocalDatabaseFileSystem) Size(path string) (int64, error) {
	if lfs.hasPageOne {
		lfs.size = lfs.pageSize * 4294967294
	}

	return lfs.size, nil
}

func (lfs *LocalDatabaseFileSystem) TransactionTimestamp() int64 {
	return 0
}

func (lfs *LocalDatabaseFileSystem) Truncate(path string, size int64) error {
	path = strings.ReplaceAll(path, ".db", "")

	// No-op since pages are stored in separate files and we don't need to
	// truncate the database "file" to a certain size.
	if size > 0 {
		return nil
	}

	// Remove all the files from the directory
	err := lfs.fileSystem.RemoveAll(fmt.Sprintf("%s/%s", lfs.path, path))

	if err != nil {
		return err
	}

	err = lfs.fileSystem.MkdirAll(fmt.Sprintf("%s/%s", lfs.path, path), 0755)

	if err != nil {
		return err
	}

	return nil
}

func (lfs *LocalDatabaseFileSystem) WalPath(path string) string {
	return ""
}

func (lfs *LocalDatabaseFileSystem) WithTransactionTimestamp(timestamp int64) DatabaseFileSystem {
	// No-op
	return lfs
}

func (lfs *LocalDatabaseFileSystem) WithWriteHook(hook func(path string, offset int64, data []byte)) DatabaseFileSystem {
	lfs.writeHook = hook

	return lfs
}

func (lfs *LocalDatabaseFileSystem) WriteAt(path string, data []byte, offset int64) (n int, err error) {
	lfs.mutex.Lock()
	defer lfs.mutex.Unlock()

	pageNumber := file.PageNumber(offset, lfs.pageSize)

	rangeFile, err := lfs.getRangeFile(path, pageNumber)

	if err != nil {
		log.Println("Error getting range file", err)
		return 0, err
	}

	pageRangeOffset := file.PageRangeOffset(pageNumber, lfs.pagesPerRange, lfs.pageSize)

	n, err = rangeFile.WriteAt(data, pageRangeOffset)

	if err != nil {
		log.Println("Error writing page", pageNumber, err)
		return 0, err
	}

	if lfs.writeHook != nil {
		lfs.writeHook(path, offset, data)
	}

	if pageNumber == 1 {
		lfs.hasPageOne = true
	}

	return n, nil
}
