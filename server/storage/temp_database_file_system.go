package storage

import (
	"fmt"
	internalStorage "litebase/internal/storage"
	"log"
	"os"
	"sync"
)

type TempDatabaseFileSystem struct {
	files        map[string]internalStorage.File
	fileSystem   *LocalFileSystemDriver
	mutex        *sync.RWMutex
	path         string
	pageSize     int64
	walTimestamp int64
	writeHook    func(offset int64, data []byte)
}

func NewTempDatabaseFileSystem(path, databaseUuid, branchUuid string, pageSize int64) DatabaseFileSystem {
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

	return &TempDatabaseFileSystem{
		files:      make(map[string]internalStorage.File),
		fileSystem: fs,
		mutex:      &sync.RWMutex{},
		path:       path,
		pageSize:   pageSize,
	}
}

func (tfs *TempDatabaseFileSystem) Close(path string) error {
	tfs.mutex.Lock()
	defer tfs.mutex.Unlock()

	file, ok := tfs.files[path]

	if !ok {
		return os.ErrNotExist
	}

	delete(tfs.files, path)

	return file.Close()
}

func (tfs *TempDatabaseFileSystem) Delete(path string) error {
	tfs.mutex.Lock()
	defer tfs.mutex.Unlock()

	file, ok := tfs.files[path]

	if ok {
		delete(tfs.files, path)
		file.Close()
	}

	tfs.fileSystem.Remove(fmt.Sprintf("%s/%s", tfs.path, path))

	return nil
}

func (tfs *TempDatabaseFileSystem) Exists() bool {
	_, err := tfs.fileSystem.Stat(tfs.path)

	return err == nil
}

func (tfs *TempDatabaseFileSystem) Open(path string) (internalStorage.File, error) {
	var filePath = fmt.Sprintf("%s/%s", tfs.path, path)

	// if tfs.walTimestamp > 0 {
	// 	filePath = fmt.Sprintf("%s_%d", filePath, tfs.walTimestamp)
	// 	log.Println("Opening file with timestamp", filePath)
	// }

	file, err := tfs.fileSystem.OpenFile(
		filePath,
		os.O_RDWR|os.O_CREATE,
		0644,
	)

	if err != nil {
		log.Println("Error opening file", err)
		return nil, err
	}

	tfs.mutex.Lock()
	tfs.files[path] = file
	tfs.mutex.Unlock()

	return file, nil
}

func (tfs *TempDatabaseFileSystem) Path() string {
	return tfs.path
}

func (tfs *TempDatabaseFileSystem) ReadAt(path string, data []byte, offset, len int64) (int, error) {
	tfs.mutex.RLock()
	file, ok := tfs.files[path]
	tfs.mutex.RUnlock()

	if !ok {
		return 0, os.ErrNotExist
	}

	n, err := file.ReadAt(data, offset)

	if err != nil {
		return 0, err
	}

	if int64(n) != len {
		return 0, fmt.Errorf("ReadAt: short read: got %d, expected %d", n, len)
	}

	return n, nil
}

func (tfs *TempDatabaseFileSystem) SetTransactionTimestamp(timestamp int64) {
	tfs.walTimestamp = timestamp
}

func (tfs *TempDatabaseFileSystem) Size(path string) (int64, error) {
	stat, err := tfs.fileSystem.Stat(fmt.Sprintf("%s/%s", tfs.path, path))

	if err != nil {
		return 0, err
	}

	return stat.Size, nil
}

func (tfs *TempDatabaseFileSystem) TransactionTimestamp() int64 {
	return tfs.walTimestamp
}

func (tfs *TempDatabaseFileSystem) Truncate(path string, size int64) error {
	err := tfs.fileSystem.Truncate(fmt.Sprintf("%s/%s", tfs.path, path), size)

	if err != nil {
		return err
	}

	return nil
}

func (tfs *TempDatabaseFileSystem) WalPath(filename string) string {
	if tfs.walTimestamp > 0 {
		filename = fmt.Sprintf("%s_%d", filename, tfs.walTimestamp)
		log.Println("WAL PATH FOR file with timestamp", filename)
	}

	return fmt.Sprintf("%s/%s", tfs.path, filename)
}

func (tfs *TempDatabaseFileSystem) WithWriteHook(hook func(offset int64, data []byte)) DatabaseFileSystem {
	tfs.writeHook = hook

	return tfs
}

func (tfs *TempDatabaseFileSystem) WithTransactionTimestamp(timestamp int64) DatabaseFileSystem {
	tfs.walTimestamp = timestamp

	return tfs
}

func (tfs *TempDatabaseFileSystem) WriteAt(path string, data []byte, offset int64) (n int, err error) {
	tfs.mutex.RLock()
	file, ok := tfs.files[path]
	tfs.mutex.RUnlock()

	if !ok {
		return 0, os.ErrNotExist
	}

	n, err = file.WriteAt(data, offset)

	if err != nil {
		return 0, err
	}

	if tfs.writeHook != nil {
		tfs.writeHook(offset, data)
	}

	return
}

func (tfs *TempDatabaseFileSystem) WriteHook(offset int64, data []byte) {
	// No-op
}
