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
				mock.DatabaseId,
				mock.BranchId,
				mock.DatabaseKey.Key,
			)

			if databaseKey == nil {
				t.Fatal("database key is nil")
			}

			if databaseKey.DatabaseId != mock.DatabaseId {
				t.Errorf("expected DatabaseId %s, got %s", mock.DatabaseId, databaseKey.DatabaseId)
			}

			if databaseKey.BranchId != mock.BranchId {
				t.Errorf("expected BranchId %s, got %s", mock.BranchId, databaseKey.BranchId)
			}

			if databaseKey.Key != mock.DatabaseKey.Key {
				t.Errorf("expected Key %s, got %s", mock.DatabaseKey.Key, databaseKey.Key)
			}
		})

		t.Run("Encode", func(t *testing.T) {
			mock := test.MockDatabase(app)

			databaseKey := auth.NewDatabaseKey(
				mock.DatabaseId,
				mock.BranchId,
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

			databaseIdBytes, _ := uuid.Parse(mock.DatabaseId)

			if !bytes.Equal(encodedKey[auth.DatabaseKeyKeySize+auth.DatabaseKeyHashSize:auth.DatabaseKeyKeySize+auth.DatabaseKeyHashSize+auth.DatabaseKeyDatabaseIdSize], databaseIdBytes[:]) {
				t.Errorf("expected DatabaseId %s, got %s", mock.DatabaseId, string(encodedKey[auth.DatabaseKeyKeySize+auth.DatabaseKeyHashSize:auth.DatabaseKeyKeySize+auth.DatabaseKeyHashSize+auth.DatabaseKeyDatabaseIdSize]))
			}

			branchIdBytes, _ := uuid.Parse(mock.BranchId)

			if !bytes.Equal(encodedKey[auth.DatabaseKeyKeySize+auth.DatabaseKeyHashSize+auth.DatabaseKeyDatabaseIdSize:], branchIdBytes[:]) {
				t.Errorf("expected BranchId %s, got %s", mock.BranchId, string(encodedKey[auth.DatabaseKeyKeySize+auth.DatabaseKeyHashSize+auth.DatabaseKeyDatabaseIdSize:]))
			}
		})

		t.Run("Decode", func(t *testing.T) {
			mock := test.MockDatabase(app)

			databaseKey := auth.NewDatabaseKey(
				mock.DatabaseId,
				mock.BranchId,
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

			if decodedKey.DatabaseId != mock.DatabaseId {
				t.Errorf("expected DatabaseId %s, got %s", mock.DatabaseId, decodedKey.DatabaseId)
			}

			if decodedKey.BranchId != mock.BranchId {
				t.Errorf("expected BranchId %s, got %s", mock.BranchId, decodedKey.BranchId)
			}
		})
	})
}
