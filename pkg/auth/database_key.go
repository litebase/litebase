package auth

import (
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/litebase/litebase/pkg/file"
)

const (
	DatabaseKeyKeySize        = 16
	DatabaseKeyHashSize       = 32
	DatabaseKeyDatabaseIdSize = 16
	DatabaseKeyBranchIdSize   = 16
	DatabaseKeySize           = DatabaseKeyKeySize + DatabaseKeyHashSize + DatabaseKeyDatabaseIdSize + DatabaseKeyBranchIdSize
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
	Key          string `json:"key"`
	DatabaseHash string `json:"database_hash"`
	DatabaseId   string `json:"database_id"`
	BranchId     string `json:"branch_id"`
}

func NewDatabaseKey(databaseId, branchId, key string) *DatabaseKey {
	return &DatabaseKey{
		Key:          key,
		DatabaseHash: file.DatabaseHash(databaseId, branchId),
		DatabaseId:   databaseId,
		BranchId:     branchId,
	}
}

func (d *DatabaseKey) Encode() ([]byte, error) {
	hashBytes, err := hex.DecodeString(d.DatabaseHash)

	if err != nil {
		return nil, err
	}

	if len(hashBytes) != DatabaseKeyHashSize {
		return nil, fmt.Errorf("invalid database hash size: expected %d bytes, got %d bytes", DatabaseKeyHashSize, len(hashBytes))
	}

	databaseIdBytes, err := uuid.Parse(d.DatabaseId)

	if err != nil {
		return nil, err
	}

	if len(databaseIdBytes) != DatabaseKeyDatabaseIdSize {
		return nil, fmt.Errorf("invalid database ID size: expected %d bytes, got %d bytes", DatabaseKeyDatabaseIdSize, len(databaseIdBytes))
	}

	branchIdBytes, err := uuid.Parse(d.BranchId)

	if err != nil {
		return nil, err
	}

	if len(branchIdBytes[:]) != DatabaseKeyBranchIdSize {
		return nil, fmt.Errorf("invalid branch ID size: expected %d bytes, got %d bytes", DatabaseKeyBranchIdSize, len(branchIdBytes[:]))
	}

	// Create a byte slice with the total fixed size
	encodedData := make([]byte, DatabaseKeyKeySize+DatabaseKeyHashSize+DatabaseKeyDatabaseIdSize+DatabaseKeyBranchIdSize)

	// Copy data into the encodedData slice, truncating or padding as necessary
	copy(encodedData[:DatabaseKeyKeySize], []byte(d.Key))                                                                                          //Database key
	copy(encodedData[DatabaseKeyKeySize:DatabaseKeyKeySize+DatabaseKeyHashSize], hashBytes)                                                        //Database hash
	copy(encodedData[DatabaseKeyKeySize+DatabaseKeyHashSize:DatabaseKeyKeySize+DatabaseKeyHashSize+DatabaseKeyDatabaseIdSize], databaseIdBytes[:]) //Database ID
	copy(encodedData[DatabaseKeyKeySize+DatabaseKeyHashSize+DatabaseKeyDatabaseIdSize:], branchIdBytes[:])                                         //Branch ID

	return encodedData, nil
}

func DecodeDatbaseKey(encodedData []byte) *DatabaseKey {
	// Create a new DatabaseKey instance
	databaseKey := &DatabaseKey{}

	// Extract the key, database hash, database ID, and branch ID from the encoded data
	databaseKey.Key = strings.TrimRight(string(encodedData[:DatabaseKeyKeySize]), "\x00")
	databaseKey.DatabaseHash = hex.EncodeToString(encodedData[DatabaseKeyKeySize : DatabaseKeyKeySize+DatabaseKeyHashSize])
	databaseUUID, _ := uuid.FromBytes(encodedData[DatabaseKeyKeySize+DatabaseKeyHashSize : DatabaseKeyKeySize+DatabaseKeyHashSize+DatabaseKeyDatabaseIdSize])
	databaseKey.DatabaseId = databaseUUID.String()
	branchUUID, _ := uuid.FromBytes(encodedData[DatabaseKeyKeySize+DatabaseKeyHashSize+DatabaseKeyDatabaseIdSize:])
	databaseKey.BranchId = branchUUID.String()

	return databaseKey
}
