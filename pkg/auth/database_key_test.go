package auth_test

import (
	"bytes"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/server"
)

func TestDatabaseKey(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		t.Run("NewDatabaseKey", func(t *testing.T) {
			mock := test.MockDatabase(app)

			databaseKey := auth.NewDatabaseKey(
				mock.DatabaseID,
				mock.BranchID,
				mock.DatabaseKey.Key,
			)

			if databaseKey == nil {
				t.Fatal("database key is nil")
			}

			if databaseKey.DatabaseID != mock.DatabaseID {
				t.Errorf("expected DatabaseID %s, got %s", mock.DatabaseID, databaseKey.DatabaseID)
			}

			if databaseKey.BranchID != mock.BranchID {
				t.Errorf("expected BranchID %s, got %s", mock.BranchID, databaseKey.BranchID)
			}

			if databaseKey.Key != mock.DatabaseKey.Key {
				t.Errorf("expected Key %s, got %s", mock.DatabaseKey.Key, databaseKey.Key)
			}
		})

		t.Run("Encode", func(t *testing.T) {
			mock := test.MockDatabase(app)

			databaseKey := auth.NewDatabaseKey(
				mock.DatabaseID,
				mock.BranchID,
				mock.DatabaseKey.Key,
			)

			encodedKey, err := databaseKey.Encode()

			if err != nil {
				t.Fatal("expected Encode to return a non-nil error")
			}

			if len(encodedKey) == 0 {
				t.Fatal("expected Encode to return a non-empty byte slice")
			}

			expectedLength := auth.DatabaseKeySize

			if len(encodedKey) != expectedLength {
				t.Fatalf("expected encoded key length %d, got %d", expectedLength, len(encodedKey))
			}

			if strings.TrimRight(string(encodedKey[:auth.DatabaseKeyKeySize]), "\x00") != mock.DatabaseKey.Key {
				t.Errorf("expected Key %s, got %s", mock.DatabaseKey.Key, string(encodedKey[:auth.DatabaseKeyKeySize]))
			}

			hashBytes, _ := hex.DecodeString(mock.DatabaseKey.DatabaseHash)

			if !bytes.Equal(encodedKey[auth.DatabaseKeyKeySize:auth.DatabaseKeyKeySize+auth.DatabaseKeyHashSize], hashBytes) {
				t.Errorf("expected DatabaseHash %s, got %s", hashBytes, string(encodedKey[auth.DatabaseKeyKeySize:auth.DatabaseKeyKeySize+auth.DatabaseKeyHashSize]))
			}

			databaseIdBytes, _ := uuid.Parse(mock.DatabaseID)

			if !bytes.Equal(encodedKey[auth.DatabaseKeyKeySize+auth.DatabaseKeyHashSize:auth.DatabaseKeyKeySize+auth.DatabaseKeyHashSize+auth.DatabaseKeyDatabaseIDSize], databaseIdBytes[:]) {
				t.Errorf("expected DatabaseID %s, got %s", mock.DatabaseID, string(encodedKey[auth.DatabaseKeyKeySize+auth.DatabaseKeyHashSize:auth.DatabaseKeyKeySize+auth.DatabaseKeyHashSize+auth.DatabaseKeyDatabaseIDSize]))
			}

			branchIdBytes, _ := uuid.Parse(mock.BranchID)

			if !bytes.Equal(encodedKey[auth.DatabaseKeyKeySize+auth.DatabaseKeyHashSize+auth.DatabaseKeyDatabaseIDSize:], branchIdBytes[:]) {
				t.Errorf("expected BranchID %s, got %s", mock.BranchID, string(encodedKey[auth.DatabaseKeyKeySize+auth.DatabaseKeyHashSize+auth.DatabaseKeyDatabaseIDSize:]))
			}
		})

		t.Run("Decode", func(t *testing.T) {
			mock := test.MockDatabase(app)

			databaseKey := auth.NewDatabaseKey(
				mock.DatabaseID,
				mock.BranchID,
				mock.DatabaseKey.Key,
			)

			encodedKey, err := databaseKey.Encode()

			if err != nil {
				t.Fatal("expected Encode to return a non-nil error")
			}

			decodedKey := auth.DecodeDatbaseKey(encodedKey)

			if decodedKey == nil {
				t.Fatal("expected DecodeDatbaseKey to return a non-nil database key")
			}

			if decodedKey.Key != mock.DatabaseKey.Key {
				t.Errorf("expected Key %s, got %s", mock.DatabaseKey.Key, decodedKey.Key)
			}

			if decodedKey.DatabaseHash != mock.DatabaseKey.DatabaseHash {
				t.Errorf("expected DatabaseHash %s, got %s", mock.DatabaseKey.DatabaseHash, decodedKey.DatabaseHash)
			}

			if decodedKey.DatabaseID != mock.DatabaseID {
				t.Errorf("expected DatabaseID %s, got %s", mock.DatabaseID, decodedKey.DatabaseID)
			}

			if decodedKey.BranchID != mock.BranchID {
				t.Errorf("expected BranchID %s, got %s", mock.BranchID, decodedKey.BranchID)
			}
		})
	})
}
