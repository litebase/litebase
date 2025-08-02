package auth

import (
	"github.com/litebase/litebase/pkg/file"
)

const (
	DatabaseKeyHashSize       = 32
	DatabaseKeyDatabaseIDSize = 16
	DatabaseKeyBranchIDSize   = 16
	DatabaseKeyBaseSize       = DatabaseKeyHashSize + DatabaseKeyDatabaseIDSize + DatabaseKeyBranchIDSize
)

// DatabaseKey represents a database key with its associated metadata. This
// data structure associates a string that is used to access the database with
// the database hash, database ID, and branch ID.
//
// When encoded as binary, the database key is stored in a file with the following
// structure:
// - 16 bytes for the key
// - 32 bytes for the database hash
// - 16 bytes for the database ID
// - 16 bytes for the branch ID
type DatabaseKey struct {
	DatabaseBranchID   string `json:"database_branch_id"`
	DatabaseBranchName string `json:"database_branch_name"`
	DatabaseHash       string `json:"database_hash"`
	DatabaseID         string `json:"database_id"`
	DatabaseName       string `json:"database_name"`
}

func NewDatabaseKey(databaseID, databaseName, branchID, branchName string) *DatabaseKey {
	return &DatabaseKey{
		DatabaseHash:       file.DatabaseHash(databaseID, branchID),
		DatabaseBranchID:   branchID,
		DatabaseBranchName: branchName,
		DatabaseID:         databaseID,
		DatabaseName:       databaseName,
	}
}
