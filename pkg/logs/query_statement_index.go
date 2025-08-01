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
	cache *cache.LFUCache
	file  internalStorage.File
	mutex *sync.Mutex
	path  string
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
		cache: cache.NewLFUCache(1000),
		file:  file,
		mutex: &sync.Mutex{},
		path:  path,
	}, nil
}

func (q *QueryStatementIndex) Close() error {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	return q.file.Close()
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

	err = q.cache.Put(key, slices.Clone(value))

	if err != nil {
		slog.Error("Failed to put entry in cache", "error", err)
	}

	return value, true
}

func (q *QueryStatementIndex) Set(key string, value string) error {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	// Write the entry to the file.
	_, err := q.file.WriteString(fmt.Sprintf("%s %s\n", key, value))

	if err != nil {
		return err
	}

	err = q.cache.Put(key, []byte(value))

	if err != nil {
		slog.Error("Failed to put entry in cache", "error", err)
	}

	return nil
}
