package storage

import (
	"crypto/sha256"
	"fmt"

	internalStorage "litebasedb/internal/storage"
	"log"
	"net/http"
	"sync"
	"time"
)

type RemoteDatabaseFileSystem struct {
	branchUuid   string
	databaseUuid string
	connection   *StorageConnection
	client       *http.Client
	hasPageOne   bool
	mutex        *sync.RWMutex
	pageCache    *PageCache
	pageSize     int64
	size         int64
}

func NewRemoteDatabaseFileSystem(
	tmpPath string,
	databaseUuid string,
	branchUuid string,
	pageSize int64,
) *RemoteDatabaseFileSystem {
	fs := &RemoteDatabaseFileSystem{
		branchUuid:   branchUuid,
		connection:   NewStorageConnection("http://localhost:8082/connection"),
		client:       &http.Client{},
		databaseUuid: databaseUuid,
		hasPageOne:   false,
		mutex:        &sync.RWMutex{},
		pageCache:    NewPageCache(tmpPath, databaseUuid, branchUuid, pageSize),
		pageSize:     pageSize,
		size:         0,
	}

	return fs
}

func (fs *RemoteDatabaseFileSystem) Close(path string) error {
	return nil
}

func (fs *RemoteDatabaseFileSystem) Delete(file string) error {
	url := fmt.Sprintf("%s/databases/%s/%s/%s", getStorageUrl(), fs.databaseUuid, fs.branchUuid, file)

	request, err := http.NewRequest("DELETE", url, nil)

	if err != nil {
		return err
	}

	_, err = fs.client.Do(request)

	if err != nil {
		return err
	}

	return nil
}

func (fs *RemoteDatabaseFileSystem) getFileSize() {
	if fs.hasPageOne {
		fs.size = fs.pageSize * 4294967294
		return
	}

	fs.size = 0 * fs.pageSize
}

func getStorageUrl() string {
	return "http://localhost:8082"
}

func (fs *RemoteDatabaseFileSystem) Open(path string) (internalStorage.File, error) {
	return nil, nil
}

func (fs *RemoteDatabaseFileSystem) Path() string {
	return ""
}

func (fs *RemoteDatabaseFileSystem) ReadAt(file string, offset int64, length int64) (data []byte, err error) {
	// start := timer.Start("READ PAGE")
	// defer timer.Stop(start)

	pageNumber := PageNumber(offset, fs.pageSize)

	if fs.pageCache.Has(offset) {
		readStart := time.Now()
		data, err = fs.pageCache.Get(offset)

		if err != nil {
			return nil, err
		}
		log.Println("Read from cache", pageNumber, time.Since(readStart))
		if len(data) > 0 {
			return data, nil
		}
	}

	response, err := fs.connection.Send(internalStorage.StorageRequest{
		BranchUuid:   fs.branchUuid,
		Command:      "READ",
		DatabaseUuid: fs.databaseUuid,
		Key:          fmt.Sprintf("%x", sha256.Sum256([]byte(fmt.Sprintf("%s/%s/%s/%d", fs.databaseUuid, fs.branchUuid, file, pageNumber)))),
		Page:         pageNumber,
	})

	if err != nil {
		log.Fatalln("Error sending request:", err)
		return nil, err
	}

	data = response.Data

	if len(data) == int(fs.pageSize) && pageNumber == 1 {
		fs.hasPageOne = true
	}

	if len(data) == int(fs.pageSize) {
		fs.pageCache.Put(offset, data)
	}

	return data, err
}

func (fs *RemoteDatabaseFileSystem) WriteAt(file string, data []byte, offset int64) (n int, err error) {
	// start := timer.Start("WRITE PAGE")
	fs.mutex.Lock()
	defer fs.mutex.Unlock()

	pageNumber := PageNumber(offset, fs.pageSize)

	_, err = fs.connection.Send(internalStorage.StorageRequest{
		BranchUuid:   fs.branchUuid,
		Command:      "WRITE",
		Data:         data,
		DatabaseUuid: fs.databaseUuid,
		Key:          fmt.Sprintf("%x", sha256.Sum256([]byte(fmt.Sprintf("%s/%s/%s/%d", fs.databaseUuid, fs.branchUuid, file, pageNumber)))),
		Page:         pageNumber,
	})

	if err != nil {
		return 0, err
	}

	// timer.Stop(start)

	if len(data) == int(fs.pageSize) {
		fs.pageCache.Put(offset, data)
	}

	return len(data), err
}

func (fs *RemoteDatabaseFileSystem) Size(file string) (int64, error) {
	fs.mutex.RLock()
	defer fs.mutex.RUnlock()

	if fs.size == 0 {
		fs.getFileSize()
	}

	// if fs.size > 0 {
	// 	maxPages := int64(fs.size / fs.pageSize)
	// 	newPages := 0

	// 	// for pageNumber := range fs.wal.pages {
	// 	// 	if pageNumber > maxPages {
	// 	// 		newPages++
	// 	// 	}
	// 	// }

	// 	totalPages := int64(newPages) + maxPages
	// 	computedSize := totalPages * fs.pageSize
	// 	fs.computedSize = computedSize

	// 	return computedSize, nil
	// }

	return fs.size, nil
}

func (fs *RemoteDatabaseFileSystem) Truncate(file string, size int64) error {
	fs.mutex.Lock()
	defer fs.mutex.Unlock()

	request, err := http.NewRequest("POST", fmt.Sprintf("%s/%s/%s/%s/truncate", getStorageUrl(), fs.databaseUuid, fs.branchUuid, file), nil)

	if err != nil {
		return err
	}

	_, err = fs.client.Do(request)

	if err != nil {
		return err
	}

	return nil
}

func PageNumber(offset, pageSize int64) int64 {
	return (offset / pageSize) + 1
}

// Calculate the offset of the page within the file
func PageOffset(offset, pageSize int64) int64 {
	pageNumber := PageNumber(offset, pageSize)

	return offset - (int64(pageNumber-1) * pageSize)
}
