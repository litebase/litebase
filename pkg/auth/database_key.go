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
	DatabaseKeyDatabaseIDSize = 16
	DatabaseKeyBranchIDSize   = 16
	DatabaseKeySize           = DatabaseKeyKeySize + DatabaseKeyHashSize + DatabaseKeyDatabaseIDSize + DatabaseKeyBranchIDSize
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
	DatabaseID   string `json:"database_id"`
	BranchID     string `json:"branch_id"`
}

func NewDatabaseKey(databaseID, branchID, key string) *DatabaseKey {
	return &DatabaseKey{
		Key:          key,
		DatabaseHash: file.DatabaseHash(databaseID, branchID),
		DatabaseID:   databaseID,
		BranchID:     branchID,
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

	databaseIDBytes, err := uuid.Parse(d.DatabaseID)

	if err != nil {
		return nil, err
	}

	if len(databaseIDBytes) != DatabaseKeyDatabaseIDSize {
		return nil, fmt.Errorf("invalid database ID size: expected %d bytes, got %d bytes", DatabaseKeyDatabaseIDSize, len(databaseIDBytes))
	}

	branchIDBytes, err := uuid.Parse(d.BranchID)

	if err != nil {
		return nil, err
	}

	if len(branchIDBytes[:]) != DatabaseKeyBranchIDSize {
		return nil, fmt.Errorf("invalid branch ID size: expected %d bytes, got %d bytes", DatabaseKeyBranchIDSize, len(branchIDBytes[:]))
	}

	// Create a byte slice with the total fixed size
	encodedData := make([]byte, DatabaseKeyKeySize+DatabaseKeyHashSize+DatabaseKeyDatabaseIDSize+DatabaseKeyBranchIDSize)

	// Copy data into the encodedData slice, truncating or padding as necessary
	copy(encodedData[:DatabaseKeyKeySize], []byte(d.Key))                                                                                          //Database key
	copy(encodedData[DatabaseKeyKeySize:DatabaseKeyKeySize+DatabaseKeyHashSize], hashBytes)                                                        //Database hash
	copy(encodedData[DatabaseKeyKeySize+DatabaseKeyHashSize:DatabaseKeyKeySize+DatabaseKeyHashSize+DatabaseKeyDatabaseIDSize], databaseIDBytes[:]) //Database ID
	copy(encodedData[DatabaseKeyKeySize+DatabaseKeyHashSize+DatabaseKeyDatabaseIDSize:], branchIDBytes[:])                                         //Branch ID

	return encodedData, nil
}

func DecodeDatbaseKey(encodedData []byte) *DatabaseKey {
	// Create a new DatabaseKey instance
	databaseKey := &DatabaseKey{}

	// Extract the key, database hash, database ID, and branch ID from the encoded data
	databaseKey.Key = strings.TrimRight(string(encodedData[:DatabaseKeyKeySize]), "\x00")
	databaseKey.DatabaseHash = hex.EncodeToString(encodedData[DatabaseKeyKeySize : DatabaseKeyKeySize+DatabaseKeyHashSize])
	databaseUUID, _ := uuid.FromBytes(encodedData[DatabaseKeyKeySize+DatabaseKeyHashSize : DatabaseKeyKeySize+DatabaseKeyHashSize+DatabaseKeyDatabaseIDSize])
	databaseKey.DatabaseID = databaseUUID.String()
	branchUUID, _ := uuid.FromBytes(encodedData[DatabaseKeyKeySize+DatabaseKeyHashSize+DatabaseKeyDatabaseIDSize:])
	databaseKey.BranchID = branchUUID.String()

	return databaseKey
}
