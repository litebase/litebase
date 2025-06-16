package auth

import (
	"encoding/binary"
	"errors"
	"io"
	"iter"
	"log"
	"log/slog"
	"os"
	"path/filepath"
	"sync"

	internalStorage "github.com/litebase/litebase/internal/storage"
	"github.com/litebase/litebase/pkg/cache"
	"github.com/litebase/litebase/pkg/storage"
)

const (
	DatabaseKeyStoreHeaderSize = 24
	DatabaseKeyStoreVersion    = 1
)

var (
	DefaultDatabaseKeyStoreCacheSize = 1000
)

// DatabaseKeyStore is a key store for database keys.
// It uses a LFU cache to store the keys in memory and a file on disk.
//
// The file is stored in the following format:
// - 4 bytes for the version
// - 4 bytes for the number of keys
// - 4 bytes for the size of the keys
// - 4 bytes for the size of the database hash
// - 4 bytes for the offset of the first free entry
//
// When a key is removed from the store, its entry is removed from the file.

type DatabaseKeyStore struct {
	cache      *cache.LFUCache
	file       internalStorage.File
	fileSystem *storage.FileSystem
	keyCount   uint32
	mutex      *sync.RWMutex
	path       string
	version    uint32
}

// Create a new instance of DatabaseKeyStore
func NewDatabaseKeyStore(
	tmpTieredFileSystem *storage.FileSystem,
	path string,
) (*DatabaseKeyStore, error) {
	dks := &DatabaseKeyStore{
		cache:      cache.NewLFUCache(DefaultDatabaseKeyStoreCacheSize),
		fileSystem: tmpTieredFileSystem,
		mutex:      &sync.RWMutex{},
		path:       path,
	}

	// Load the database keys from the file system
	err := dks.load()

	if err != nil {
		return nil, err
	}

	return dks, nil
}

// Return all of the database keys in the store
func (d *DatabaseKeyStore) All() iter.Seq[*DatabaseKey] {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	return func(yield func(*DatabaseKey) bool) {
		offset := int64(DatabaseKeyStoreHeaderSize)
		encodedKey := make([]byte, DatabaseKeySize)

		for {
			n, err := d.file.ReadAt(encodedKey, offset)
			if err != nil {
				if err == io.EOF {
					break
				}

				slog.Error("Failed to read database key:", "error", err)
				return
			}

			if n != len(encodedKey) {
				slog.Error("Unexpected key length:", "actual", n, "expected", len(encodedKey))
				break
			}

			if !yield(DecodeDatbaseKey(encodedKey)) {
				break
			}

			offset += int64(len(encodedKey))
		}
	}
}

// Close the store
func (d *DatabaseKeyStore) Close() error {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	// Write the database key store header
	err := d.writeHeader()

	if err != nil {
		return err

	}

	if d.file != nil {
		err := d.file.Close()

		if err != nil {
			return err
		}
	}

	return nil
}

// Create the database key store file
func (d *DatabaseKeyStore) create(header []byte) error {
	// Write the database key store header
	binary.LittleEndian.PutUint32(header[0:4], uint32(DatabaseKeyStoreVersion)) // version
	binary.LittleEndian.PutUint32(header[4:8], uint32(0))                       // number of keys
	binary.LittleEndian.PutUint32(header[12:16], uint32(DatabaseKeySize))       // size of the keys

	_, err := d.file.WriteAt(header, 0)

	if err != nil {
		return err
	}

	return nil
}

// Delete a key in the store
func (d *DatabaseKeyStore) Delete(key string) error {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	keyFound := false

	d.cache.Delete(key)

	offset := int64(DatabaseKeyStoreHeaderSize)
	encodedKey := make([]byte, DatabaseKeySize)
	var readKey *DatabaseKey

	for {
		n, err := d.file.ReadAt(encodedKey, offset)

		if err != nil {
			if err == io.EOF {
				break
			}

			return err
		}

		if len(encodedKey) != DatabaseKeySize {
			break
		}

		readKey = DecodeDatbaseKey(encodedKey)

		if readKey.Key == key {
			keyFound = true
			// Mark the entry as deleted by writing zeros
			zeroKey := make([]byte, DatabaseKeySize)

			_, err := d.file.WriteAt(zeroKey, offset)

			if err != nil {
				return err
			}

			d.keyCount--

			err = d.writeHeader()

			if err != nil {
				return err
			}

			break
		}

		offset += int64(n)
	}

	if !keyFound {
		return errors.New("database key not found")
	}

	return nil
}

// Get a key from the store
func (d *DatabaseKeyStore) Get(key string) (*DatabaseKey, error) {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	if databaseKey, found := d.cache.Get(key); found {
		return databaseKey.(*DatabaseKey), nil
	}

	var offset int64 = DatabaseKeyStoreHeaderSize
	encodedKey := make([]byte, DatabaseKeySize)

	for {
		n, err := d.file.ReadAt(encodedKey, offset)

		if err != nil {
			if err == io.EOF {
				break
			}

			return nil, err
		}

		if n != len(encodedKey) {
			log.Println("Unexpected key length:", n, "expected:", len(encodedKey))
			break
		}

		databaseKey := DecodeDatbaseKey(encodedKey)

		if databaseKey.Key == key {
			err := d.cache.Put(key, databaseKey)

			if err != nil {
				slog.Error("Failed to cache database key:", "error", err)
			}

			return databaseKey, nil
		}

		offset += int64(len(encodedKey))
	}

	return nil, errors.New("database key not found")
}

// Get the number of keys in the store.
func (d *DatabaseKeyStore) Len() uint32 {
	d.mutex.RLock()
	defer d.mutex.RUnlock()

	return d.keyCount
}

// Load the database key store from the file system.
func (d *DatabaseKeyStore) load() error {
tryOpen:
	file, err := d.fileSystem.OpenFile(d.path, os.O_RDWR|os.O_CREATE, 0600)

	if err != nil {
		if os.IsNotExist(err) {
			err := d.fileSystem.MkdirAll(filepath.Dir(d.path), 0750)

			if err != nil {
				return err
			}

			goto tryOpen
		}

		return err
	}

	d.file = file

	// Read the database key store header
	header := make([]byte, DatabaseKeyStoreHeaderSize)
	_, err = d.file.ReadAt(header, 0)

	if err != nil {
		if err == io.EOF {
			// File is empty, create a new one
			err = d.create(header)

			if err != nil {
				return err
			}
		} else {
			return err
		}
	}

	d.version = binary.LittleEndian.Uint32(header[0:4])  // version
	d.keyCount = binary.LittleEndian.Uint32(header[4:8]) // size of the keys

	if d.version != DatabaseKeyStoreVersion {
		return errors.New("invalid database key store version")
	}

	return nil
}

// Put a key in the store
func (d *DatabaseKeyStore) Put(key *DatabaseKey) error {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	var offset int64 = DatabaseKeyStoreHeaderSize
	var databaseKey *DatabaseKey = &DatabaseKey{}
	encodedKey := make([]byte, DatabaseKeySize)

	for {
		n, err := d.file.ReadAt(encodedKey, offset)

		if err != nil {
			if err == io.EOF {
				break
			}

			return err
		}

		if n != len(encodedKey) {
			log.Println("key length", n)
			break
		}

		databaseKey.Key = string(encodedKey[:DatabaseKeyKeySize])
		databaseKey.DatabaseHash = string(encodedKey[DatabaseKeyKeySize : DatabaseKeyKeySize+DatabaseKeyHashSize])

		if databaseKey.Key == key.Key {
			err := d.cache.Put(key.Key, key)

			if err != nil {
				slog.Error("Failed to cache database key:", "error", err)
			}

			return nil
		}

		if databaseKey.Key == "" && databaseKey.DatabaseHash == "" {
			encodedData, err := key.Encode()

			if err != nil {
				return err
			}

			_, err = d.file.WriteAt(encodedData, offset)

			if err != nil {
				return err
			}

			err = d.cache.Put(key.Key, key)

			if err != nil {
				slog.Error("Failed to cache database key:", "error", err)
			}

			return nil
		}

		offset += int64(len(encodedKey))
	}

	encodedKey, err := key.Encode()

	if err != nil {
		return err
	}

	// Write the key to the file
	_, err = d.file.WriteAt(encodedKey, offset)

	if err != nil {
		return err
	}

	if err := d.cache.Put(key.Key, key); err != nil {
		return err
	}

	// Update the free offset
	d.keyCount++

	err = d.writeHeader()

	if err != nil {
		slog.Error("Failed to write database key store header:", "error", err)
	}

	return err
}

// Write the header of the store to the file.
func (d *DatabaseKeyStore) writeHeader() error {
	header := make([]byte, DatabaseKeyStoreHeaderSize)

	// Write the database key store header
	binary.LittleEndian.PutUint32(header[0:4], uint32(DatabaseKeyStoreVersion)) // version
	binary.LittleEndian.PutUint32(header[4:8], uint32(d.keyCount))              // number of keys
	binary.LittleEndian.PutUint32(header[12:16], uint32(DatabaseKeySize))       // size of the keys

	_, err := d.file.WriteAt(header, 0)

	if err != nil {
		return err
	}

	return nil
}
