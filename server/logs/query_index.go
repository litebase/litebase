package logs

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	internalStorage "litebase/internal/storage"
	"litebase/server/cache"
	"litebase/server/storage"
	"log"
	"os"
	"sync"
)

/*
The Query Index stores the queries that have been executed on the database.
These entries are associated with a hash that associates the query with Query
Log entries.
*/

type QueryIndex struct {
	cache *cache.LFUCache
	file  internalStorage.File
	mutex *sync.Mutex
	path  string
}

func GetQueryIndex(tieredFS *storage.FileSystem, path, name string, timestamp int64) (*QueryIndex, error) {
	directoryPath := fmt.Sprintf("%s/%d", path, timestamp)
	indexPath := fmt.Sprintf("%s/%d/%s", path, timestamp, name)

	err := tieredFS.MkdirAll(directoryPath, 0755)

	if err != nil {
		log.Fatal(err)
	}

	file, err := tieredFS.OpenFile(indexPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)

	if err != nil {
		log.Fatalln("Failed to open file", err)
		return nil, err
	}

	return &QueryIndex{
		cache: cache.NewLFUCache(1000),
		file:  file,
		mutex: &sync.Mutex{},
		path:  path,
	}, nil
}

func (q *QueryIndex) Close() error {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	return q.file.Close()
}

func (q *QueryIndex) Get(key string) ([]byte, bool) {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	entry, ok := q.cache.Get(key)

	if ok {
		return entry, true
	}

	// Reset the file pointer to the beginning
	_, err := q.file.Seek(0, io.SeekStart)

	if err != nil {
		log.Fatalf("Failed to seek file: %v", err)
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

	q.cache.Put(key, value)

	return value, true
}

// func (q *QueryIndex) Has(key string) bool {
// 	q.mutex.Lock()
// 	defer q.mutex.Unlock()

// 	if _, ok := q.cache.Get(key); ok {
// 		log.Println("Cache hit")
// 		return true
// 	}

// 	// Reset the file pointer to the beginning
// 	_, err := q.file.Seek(0, io.SeekStart)

// 	if err != nil {
// 		log.Fatalf("Failed to seek file: %v", err)
// 	}

// 	scanner := bufio.NewScanner(q.file)

// 	for scanner.Scan() {
// 		line := scanner.Bytes()

// 		// Skip empty lines
// 		if len(line) == 0 {
// 			continue
// 		}

// 		// Split the line into key and value, the key and value are separated by
// 		// the first space
// 		data := bytes.SplitN(line, []byte(" "), 2)
// 		hash := data[0]

// 		if bytes.Equal([]byte(key), hash) {
// 			return true
// 		}
// 	}

// 	return false
// }

// TODO: Implement file locking to allow multiple writers from different nodes.
func (q *QueryIndex) Set(key string, value string) error {
	q.mutex.Lock()
	defer q.mutex.Unlock()

	// Write the entry to the file.
	_, err := q.file.WriteString(fmt.Sprintf("%s %s\n", key, value))

	if err != nil {
		return err
	}

	q.cache.Put(key, []byte(value))

	return nil
}
