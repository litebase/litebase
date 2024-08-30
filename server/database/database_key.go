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
	DatabaseUuid string `json:"database_uuid"`
	BranchUuid   string `json:"branch_uuid"`
}

var databaseKeyCache = map[string]*DatabaseKey{}
var databaseKeyMutex = &sync.RWMutex{}

func NewDatabaseKey(databaseUuid, branchUuid string) *DatabaseKey {
	return &DatabaseKey{
		DatabaseHash: file.DatabaseHash(databaseUuid, branchUuid),
		DatabaseUuid: databaseUuid,
		BranchUuid:   branchUuid,
	}
}

func GetDatabaseKey(key string) (*DatabaseKey, error) {
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
	data, err := storage.ObjectFS().ReadFile(auth.GetDatabaseKeyPath(config.Get().Signature, key))

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

func GetDatabaseKeyCount() int64 {
	// Read all files in the databases directory
	entries, err := storage.ObjectFS().ReadDir(auth.GetDatabaseKeysPath(config.Get().Signature) + "/")

	if err != nil {
		return 0
	}

	return int64(len(entries))
}
