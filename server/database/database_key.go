package database

import (
	"encoding/json"
	"litebasedb/internal/config"
	"litebasedb/server/auth"
	"litebasedb/server/storage"
	"sync"
)

type DatabaseKey struct {
	Key          string `json:"key"`
	DatabaseUuid string `json:"database_uuid"`
	BranchUuid   string `json:"branch_uuid"`
}

var databaseKeyCache = map[string]*DatabaseKey{}
var databaseKeyMutex = &sync.RWMutex{}

func GetDatabaseKey(key string) (*DatabaseKey, error) {
	// Check if the database key is cached
	databaseKeyMutex.RLock()

	if databaseKey, ok := databaseKeyCache[key]; ok {
		databaseKeyMutex.RUnlock()
		return databaseKey, nil
	}

	databaseKeyMutex.RUnlock()

	// Read the database key file
	data, err := storage.FS().ReadFile(auth.GetDatabaseKeyPath(config.Get().Signature, key))

	if err != nil {
		return nil, err
	}

	databaseKey := &DatabaseKey{}

	err = json.Unmarshal(data, databaseKey)

	if err != nil {
		return nil, err
	}

	// Cache the database key
	databaseKeyMutex.Lock()
	databaseKeyCache[key] = databaseKey
	databaseKeyMutex.Unlock()

	return databaseKey, nil
}

func GetDatabaseKeyCount() int {
	// Read all files in the databases directory
	entries, err := storage.FS().ReadDir(auth.GetDatabaseKeysPath(config.Get().Signature))

	if err != nil {
		return 0
	}

	return len(entries)
}
