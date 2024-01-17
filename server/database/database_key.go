package database

import (
	"encoding/json"
	"litebasedb/internal/config"
	"litebasedb/server/auth"
	"litebasedb/server/storage"
)

type DatabaseKey struct {
	Key          string `json:"key"`
	DatabaseUuid string `json:"database_uuid"`
	BranchUuid   string `json:"branch_uuid"`
}

func GetDatabaseKey(key string) (*DatabaseKey, error) {
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
