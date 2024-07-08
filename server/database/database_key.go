package database

import (
	"bytes"
	"encoding/json"
	"litebase/internal/config"
	"litebase/server/auth"
	"litebase/server/storage"
	"sync"
)

type DatabaseKey struct {
	Key          string `json:"key"`
	DatabaseUuid string `json:"database_uuid"`
	BranchUuid   string `json:"branch_uuid"`
}

var databaseKeyCache = map[string]DatabaseKey{}
var databaseKeyMutex = &sync.RWMutex{}

func GetDatabaseKey(key string) (DatabaseKey, error) {
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
	data, err := storage.FS().ReadFile(auth.GetDatabaseKeyPath(config.Get().Signature, key))

	if err != nil {
		return DatabaseKey{}, err
	}

	databaseKey := DatabaseKey{}

	err = json.NewDecoder(bytes.NewReader(data)).Decode(&databaseKey)

	if err != nil {
		return DatabaseKey{}, err
	}

	// Cache the database key
	databaseKeyCache[key] = databaseKey

	return databaseKey, nil
}

func GetDatabaseKeyCount() int {
	// Read all files in the databases directory
	// STORAGE TODO: Deprecate, we need to create a database key index file and read from there
	entries, err := storage.FS().ReadDir(auth.GetDatabaseKeysPath(config.Get().Signature))

	if err != nil {
		return 0
	}

	return len(entries)
}
