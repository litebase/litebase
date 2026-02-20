package logs

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"log/slog"
	"os"
	"slices"
	"sync"

	internalStorage "github.com/litebase/litebase/internal/storage"
	"github.com/litebase/litebase/pkg/cache"
	"github.com/litebase/litebase/pkg/storage"
)

// The Query Index stores the queries that have been executed on the database.
// These entries are associated with a hash that associates the query with Query
// Log entries.
type QueryStatementIndex struct {
	cache     *cache.LFUCache
	dataKey   []byte // Optional: 32-byte encryption key
	encrypted bool   // Whether the index file is encrypted
	file      internalStorage.File
	keyHash   [32]byte // Optional: SHA256 hash of encryption key
	mutex     *sync.Mutex
	path      string
}

func GetQueryStatementIndex(tieredFS *storage.FileSystem, path, name string, timestamp int64) (*QueryStatementIndex, error) {
	directoryPath := fmt.Sprintf("%s/%d/", path, timestamp)
	indexPath := fmt.Sprintf("%s/%s", directoryPath, name)

	err := tieredFS.MkdirAll(directoryPath, 0750)

	if err != nil {
		return nil, err
	}

	file, err := tieredFS.OpenFile(indexPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0600)

	if err != nil {
		log.Println("Failed to open file", err)
		return nil, err
	}

	return &QueryStatementIndex{
		cache:     cache.NewLFUCache(1000),
		encrypted: false,
		file:      file,
		mutex:     &sync.Mutex{},
		path:      path,
	}, nil
}

// GetQueryStatementIndexWithEncryption opens a query statement index with encryption enabled.
func GetQueryStatementIndexWithEncryption(tieredFS *storage.FileSystem, path, name string, timestamp int64, dataKey []byte, keyHash [32]byte) (*QueryStatementIndex, error) {
	if len(dataKey) != 32 {
		return nil, fmt.Errorf("dataKey must be 32 bytes, got %d", len(dataKey))
	}

	directoryPath := fmt.Sprintf("%s/%d/", path, timestamp)
	indexPath := fmt.Sprintf("%s/%s", directoryPath, name)

	err := tieredFS.MkdirAll(directoryPath, 0750)

	if err != nil {
		return nil, err
	}

	file, err := tieredFS.OpenFile(indexPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0600)

	if err != nil {
		log.Println("Failed to open file", err)
		return nil, err
	}

	return &QueryStatementIndex{
		cache:     cache.NewLFUCache(1000),
		dataKey:   dataKey,
		encrypted: true,
		file:      file,
		keyHash:   keyHash,
		mutex:     &sync.Mutex{},
		path:      path,
	}, nil
}

func (q *QueryStatementIndex) Close() error {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	return q.file.Close()
}

// ConfigureEncryption sets the encryption parameters for the QueryStatementIndex.
func (q *QueryStatementIndex) ConfigureEncryption(dataKey []byte, keyHash [32]byte) error {
	if len(dataKey) != 32 {
		return fmt.Errorf("dataKey must be 32 bytes, got %d", len(dataKey))
	}

	q.mutex.Lock()
	defer q.mutex.Unlock()

	q.dataKey = dataKey
	q.keyHash = keyHash
	q.encrypted = true

	return nil
}

// IsEncrypted returns whether the statement index is encrypted.
func (q *QueryStatementIndex) IsEncrypted() bool {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	return q.encrypted
}

func (q *QueryStatementIndex) Get(key string) ([]byte, bool) {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	entry, ok := q.cache.Get(key)

	if ok {
		return entry.([]byte), true
	}

	// Reset the file pointer to the beginning
	_, err := q.file.Seek(0, io.SeekStart)

	if err != nil {
		// TODO: Handle this error
		log.Printf("Failed to seek file: %v\n", err)
	}

	// Read the entry from the file.
	scanner := bufio.NewScanner(q.file)
	var value []byte

	for scanner.Scan() {
		line := scanner.Bytes()

		// Skip empty lines
		if len(line) == 0 {
			continue
		}

		// Split the line into key and value, the key and value are separated by a space
		data := bytes.SplitN(line, []byte(" "), 2)
		hash := data[0]

		if bytes.Equal([]byte(key), hash) {
			value = data[1]
			break
		}
	}

	if value == nil {
		return nil, false
	}

	_, _, err = q.cache.Put(key, slices.Clone(value))

	if err != nil {
		slog.Error("Failed to put entry in cache", "error", err)
	}

	return value, true
}

func (q *QueryStatementIndex) Set(key string, value string) error {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	// Write the entry to the file.
	_, err := fmt.Fprintf(q.file, "%s %s\n", key, value)

	if err != nil {
		return err
	}

	_, _, err = q.cache.Put(key, []byte(value))

	if err != nil {
		slog.Error("Failed to put entry in cache", "error", err)
	}

	return nil
}
