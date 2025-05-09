package auth

import (
	"github.com/litebase/litebase/common/config"
	"github.com/litebase/litebase/server/file"
	"github.com/litebase/litebase/server/storage"
)

type DatabaseKey struct {
	Key          string `json:"key"`
	DatabaseHash string `json:"database_hash"`
	DatabaseId   string `json:"database_id"`
	BranchId     string `json:"branch_id"`
}

func NewDatabaseKey(databaseId, branchId string) *DatabaseKey {
	return &DatabaseKey{
		DatabaseHash: file.DatabaseHash(databaseId, branchId),
		DatabaseId:   databaseId,
		BranchId:     branchId,
	}
}

func GetDatabaseKeyCount(c *config.Config, objectFS *storage.FileSystem) int64 {
	// Read all files in the databases directory
	entries, err := objectFS.ReadDir(GetDatabaseKeysPath(c.Signature))

	if err != nil {
		return 0
	}

	return int64(len(entries))
}
