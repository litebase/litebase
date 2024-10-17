package database

import (
	"bytes"
	"encoding/json"
	"litebase/internal/config"
	"litebase/server/auth"
	"litebase/server/file"
	"litebase/server/storage"
	"sync"
)

type DatabaseKey struct {
	Key          string `json:"key"`
	DatabaseHash string `json:"database_hash"`
	DatabaseId   string `json:"database_id"`
	BranchId     string `json:"branch_id"`
}

var databaseKeyCache = map[string]*DatabaseKey{}
var databaseKeyMutex = &sync.RWMutex{}

func NewDatabaseKey(databaseId, branchId string) *DatabaseKey {
	return &DatabaseKey{
		DatabaseHash: file.DatabaseHash(databaseId, branchId),
		DatabaseId:   databaseId,
		BranchId:     branchId,
	}
}

func GetDatabaseKey(c *config.Config, objectFS *storage.FileSystem, key string) (*DatabaseKey, error) {
	// Check if the database key is cached
	databaseKeyMutex.RLock()

	if databaseKey, ok := databaseKeyCache[key]; ok {
		databaseKeyMutex.RUnlock()
		return databaseKey, nil
	}

	databaseKeyMutex.RUnlock()

	databaseKeyMutex.Lock()
	defer databaseKeyMutex.Unlock()

	// Read the database key file
	data, err := objectFS.ReadFile(auth.GetDatabaseKeyPath(c.Signature, key))

	if err != nil {
		return nil, err
	}

	databaseKey := &DatabaseKey{}

	err = json.NewDecoder(bytes.NewReader(data)).Decode(&databaseKey)

	if err != nil {
		return nil, err
	}

	// Cache the database key
	databaseKeyCache[key] = databaseKey

	return databaseKey, nil
}

func GetDatabaseKeyCount(c *config.Config, objectFS *storage.FileSystem) int64 {
	// Read all files in the databases directory
	entries, err := objectFS.ReadDir(auth.GetDatabaseKeysPath(c.Signature) + "/")

	if err != nil {
		return 0
	}

	return int64(len(entries))
}
