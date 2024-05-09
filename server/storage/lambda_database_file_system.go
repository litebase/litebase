package storage

import (
	"fmt"
	internalStorage "litebasedb/internal/storage"
	"log"
	"net/http"
	"sync"

	"github.com/klauspost/compress/s2"
)

type LambdaDatabaseFileSystem struct {
	branchUuid     string
	databaseUuid   string
	client         *http.Client
	connectionHash string
	hasPageOne     bool
	mutex          *sync.RWMutex
	pageCache      *PageCache
	pageReader     *PageReader
	pageSize       int64
	size           int64
}

func NewLambdaDatabaseFileSystem(
	connectionHash string,
	tmpPath string,
	databaseUuid string,
	branchUuid string,
	pageSize int64,
) *LambdaDatabaseFileSystem {
	fs := &LambdaDatabaseFileSystem{
		branchUuid:     branchUuid,
		client:         &http.Client{},
		connectionHash: connectionHash,
		databaseUuid:   databaseUuid,
		hasPageOne:     false,
		mutex:          &sync.RWMutex{},
		// TODO: The page cache needs to be shared between all file system instances
		pageCache: NewPageCache(tmpPath, databaseUuid, branchUuid, pageSize),
		pageSize:  pageSize,
		size:      0,
	}

	fs.pageReader = NewPageReader(fs)

	return fs
}

// No-op
func (fs *LambdaDatabaseFileSystem) Close(file string) error {
	log.Println("LambdaDatabaseFileSystem Close", file)
	return nil
}

func (fs *LambdaDatabaseFileSystem) connection() (*LambdaConnection, error) {
	return LambdaConnectionManager().Get(fs.connectionHash)
}

func (fs *LambdaDatabaseFileSystem) Delete(file string) error {
	log.Println("LambdaDatabaseFileSystem DELETE", file)
	connection, err := fs.connection()

	if err != nil {
		return err
	}

	_, err = connection.Send(internalStorage.StorageRequest{
		BranchUuid:   fs.branchUuid,
		Command:      "DELETE",
		DatabaseUuid: fs.databaseUuid,
		// Key:          fmt.Sprintf("%x", sha256.Sum256([]byte(fmt.Sprintf("%s/%s/%s", fs.databaseUuid, fs.branchUuid, file)))),
		Key: file,
	})

	defer LambdaConnectionManager().Release(fs.connectionHash, connection)

	if err != nil {
		return err
	}

	return nil
}

func (fs *LambdaDatabaseFileSystem) FetchPage(pageNumber int64) ([]byte, error) {
	connection, err := fs.connection()

	if err != nil {
		return nil, err
	}

	response, err := connection.Send(internalStorage.StorageRequest{
		BranchUuid:   fs.branchUuid,
		Command:      "READ",
		DatabaseUuid: fs.databaseUuid,
		// Key:          fmt.Sprintf("%x", sha256.Sum256([]byte(fmt.Sprintf("%s/%s/%s/%d", fs.databaseUuid, fs.branchUuid, name, pageNumber)))),
		Key:  fmt.Sprintf("%d", pageNumber),
		Page: pageNumber,
	})

	defer LambdaConnectionManager().Release(fs.connectionHash, connection)

	if err != nil {
		log.Println("Error sending request:", err)
		return nil, err
	}

	return response.Data, nil
}

func (fs *LambdaDatabaseFileSystem) getFileSize() {
	if fs.hasPageOne {
		fs.size = fs.pageSize * 4294967294
		return
	}

	fs.size = 0 * fs.pageSize
}

// No-op
func (fs *LambdaDatabaseFileSystem) Open(file string) (internalStorage.File, error) {
	return nil, nil
}

func (fs *LambdaDatabaseFileSystem) PageCache() *PageCache {
	return fs.pageCache
}

func (fs *LambdaDatabaseFileSystem) PageSize() int64 {
	return fs.pageSize
}

// No-op
func (fs *LambdaDatabaseFileSystem) Path() string {
	return ""
}

func (fs *LambdaDatabaseFileSystem) ReadAt(name string, offset int64, length int64) (data []byte, err error) {
	// start := timer.Start("READ PAGE")
	// defer timer.Stop(start)

	pageNumber := PageNumber(offset, fs.pageSize)

	if fs.pageCache.Has(offset) {
		// readStart := time.Now()
		data, err = fs.pageCache.Get(offset)

		if err != nil {
			return nil, err
		}

		// log.Println("Read from cache", pageNumber, time.Since(readStart))

		if len(data) > 0 {
			return data, nil
		}
	}

	// log.Println("Reading page", pageNumber)
	compressedData, err := fs.FetchPage(pageNumber)

	if err != nil {
		return nil, err
	}

	if len(compressedData) == 0 {
		return []byte{}, nil
	}

	data, err = fs.decompressData(compressedData)

	if err != nil {
		return nil, err
	}

	if len(data) == int(fs.pageSize) && pageNumber == 1 {
		fs.hasPageOne = true
	}

	// TODO: Read ahead only when the database connection has instructed us to do so.
	// fs.pageReader.ReadAhead(name, pageNumber, offset, data)

	// We cannot cache page 1 since it can be updated by the database
	if len(data) == int(fs.pageSize) {
		if pageNumber != 1 {
			fs.pageCache.Put(offset, data)
		}
	}

	return data, err
}

func (fs *LambdaDatabaseFileSystem) WriteAt(file string, data []byte, offset int64) (n int, err error) {
	// start := timer.Start("WRITE PAGE")
	fs.mutex.Lock()
	defer fs.mutex.Unlock()

	pageNumber := PageNumber(offset, fs.pageSize)

	connection, err := fs.connection()

	if err != nil {
		return 0, err
	}

	compressedData, err := fs.compressData(data)

	if err != nil {
		return 0, err
	}

	_, err = connection.Send(internalStorage.StorageRequest{
		BranchUuid:   fs.branchUuid,
		Command:      "WRITE",
		Data:         compressedData,
		DatabaseUuid: fs.databaseUuid,
		// Key:          fmt.Sprintf("%x", sha256.Sum256([]byte(fmt.Sprintf("%s/%s/%s/%d", fs.databaseUuid, fs.branchUuid, file, pageNumber)))),
		Key:  fmt.Sprintf("%d", pageNumber),
		Page: pageNumber,
	})

	defer LambdaConnectionManager().Release(fs.connectionHash, connection)

	if err != nil {
		return 0, err
	}

	// timer.Stop(start)

	// Only cache full pages and cache the first page if we have already calculated the size
	// of the database. Otherwise, we will get a SQLITE_CORRUPT error.
	if pageNumber == 1 && fs.size > 0 && len(data) == int(fs.pageSize) ||
		len(data) == int(fs.pageSize) {
		fs.pageCache.Put(offset, data)
	}

	return len(data), err
}

func (fs *LambdaDatabaseFileSystem) Size(file string) (int64, error) {
	fs.mutex.RLock()
	defer fs.mutex.RUnlock()

	if fs.size == 0 {
		fs.getFileSize()
	}

	return fs.size, nil
}

func (fs *LambdaDatabaseFileSystem) Truncate(file string, size int64) error {
	connection, err := fs.connection()

	if err != nil {
		return err
	}

	_, err = connection.Send(internalStorage.StorageRequest{
		BranchUuid:   fs.branchUuid,
		Command:      "TRUNCATE",
		DatabaseUuid: fs.databaseUuid,
		// Key:          fmt.Sprintf("%x", sha256.Sum256([]byte(fmt.Sprintf("%s/%s/%s", fs.databaseUuid, fs.branchUuid, file)))),
		Key:  file,
		Size: size,
	})

	defer LambdaConnectionManager().Release(fs.connectionHash, connection)

	if err != nil {
		return err
	}

	return nil
}

func (fs *LambdaDatabaseFileSystem) compressData(data []byte) ([]byte, error) {
	return s2.Encode(nil, data), nil
}

func (fs *LambdaDatabaseFileSystem) decompressData(data []byte) ([]byte, error) {
	return s2.Decode(nil, data)
}
