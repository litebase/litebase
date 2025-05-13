package auth

import (
	"github.com/litebase/litebase/common/config"
	"github.com/litebase/litebase/server/file"
	"github.com/litebase/litebase/server/storage"
)

const (
	DatabaseKeySize  = 16
	DatabaseHashSize = 20
)

// DatabaseKey represents a database key with its associated metadata. This
// data structure associates a string that is used to access the database with
// the database hash, database ID, and branch ID.
//
// When encoded as binary, the database key is stored in a file with the following
// structure:
// - 16 bytes for the key
// - 20 bytes for the database hash
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

func (d *DatabaseKey) Encode() []byte {
	// Create a byte slice with the total fixed size
	encodedData := make([]byte, DatabaseKeySize+DatabaseHashSize)

	// Copy data into the encodedData slice, truncating or padding as necessary
	copy(encodedData[:DatabaseKeySize], []byte(d.Key))
	copy(encodedData[DatabaseKeySize:DatabaseKeySize+DatabaseHashSize], []byte(d.DatabaseHash))

	return encodedData
}

func GetDatabaseKeyCount(c *config.Config, objectFS *storage.FileSystem) int64 {
	// Read all files in the databases directory
	entries, err := objectFS.ReadDir(GetDatabaseKeysPath(c.Signature))

	if err != nil {
		return 0
	}

	return int64(len(entries))
}
