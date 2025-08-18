package database_test

import (
	"strings"
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/database"
	"github.com/litebase/litebase/pkg/server"
)

func TestSystemDatabaseAccessKeys(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		// Create a test access key storage instance
		storage := database.NewSystemDatabaseAccessKeyStorage(
			app.Config,
			app.Auth.SecretsManager,
			app.DatabaseManager.SystemDatabase(),
		)

		t.Run("Store and Get", func(t *testing.T) {
			accessKey := &auth.AccessKey{
				AccessKeyID:     "test_access_key_id",
				AccessKeySecret: "test_access_key_secret",
				Description:     "Test access key",
				Statements: []auth.AccessKeyStatement{
					{
						Effect:   auth.StatementEffectAllow,
						Resource: "*",
						Actions:  []auth.Privilege{"*"},
					},
				},
				CreatedAt: time.Now().UTC(),
				UpdatedAt: time.Now().UTC(),
			}

			err := storage.Store(accessKey)

			if err != nil {
				t.Fatalf("Failed to store access key: %v", err)
			}

			// Test Get
			retrievedKey, err := storage.Get("test_access_key_id")

			if err != nil {
				t.Fatalf("Failed to get access key: %v", err)
			}

			if retrievedKey.AccessKeyID != accessKey.AccessKeyID {
				t.Errorf("Expected AccessKeyID %s, got %s", accessKey.AccessKeyID, retrievedKey.AccessKeyID)
			}

			if retrievedKey.AccessKeySecret != accessKey.AccessKeySecret {
				t.Errorf("Expected AccessKeySecret %s, got %s", accessKey.AccessKeySecret, retrievedKey.AccessKeySecret)
			}

			if retrievedKey.Description != accessKey.Description {
				t.Errorf("Expected Description %s, got %s", accessKey.Description, retrievedKey.Description)
			}

			if len(retrievedKey.Statements) != len(accessKey.Statements) {
				t.Errorf("Expected %d statements, got %d", len(accessKey.Statements), len(retrievedKey.Statements))
			} else {
				if retrievedKey.Statements[0].Effect != accessKey.Statements[0].Effect {
					t.Errorf("Expected Effect %s, got %s", accessKey.Statements[0].Effect, retrievedKey.Statements[0].Effect)
				}

				if retrievedKey.Statements[0].Resource != accessKey.Statements[0].Resource {
					t.Errorf("Expected Resource %s, got %s", accessKey.Statements[0].Resource, retrievedKey.Statements[0].Resource)
				}

				if len(retrievedKey.Statements[0].Actions) != len(accessKey.Statements[0].Actions) {
					t.Errorf("Expected %d actions, got %d", len(accessKey.Statements[0].Actions), len(retrievedKey.Statements[0].Actions))
				}
			}

			if retrievedKey.CreatedAt.Sub(accessKey.CreatedAt).Abs() > time.Second {
				t.Errorf("CreatedAt timestamp differs too much: expected %v, got %v", accessKey.CreatedAt, retrievedKey.CreatedAt)
			}

			if retrievedKey.UpdatedAt.Sub(accessKey.UpdatedAt).Abs() > time.Second {
				t.Errorf("UpdatedAt timestamp differs too much: expected %v, got %v", accessKey.UpdatedAt, retrievedKey.UpdatedAt)
			}
		})

		t.Run("Get non-existent key", func(t *testing.T) {
			_, err := storage.Get("non_existent_key")

			if err == nil {
				t.Error("Expected error when getting non-existent key, but got nil")
			}
		})

		t.Run("Store with complex statements", func(t *testing.T) {
			accessKey := &auth.AccessKey{
				AccessKeyID:     "complex_access_key_id",
				AccessKeySecret: "complex_access_key_secret",
				Description:     "Complex access key with multiple statements",
				Statements: []auth.AccessKeyStatement{
					{
						Effect:   auth.StatementEffectAllow,
						Resource: "database:*",
						Actions:  []auth.Privilege{auth.DatabasePrivilegeRead, auth.DatabasePrivilegeSelect},
					},
					{
						Effect:   auth.StatementEffectDeny,
						Resource: "database:sensitive:*",
						Actions:  []auth.Privilege{auth.DatabasePrivilegeDelete},
					},
					{
						Effect:   auth.StatementEffectAllow,
						Resource: "database:public:table:users",
						Actions:  []auth.Privilege{auth.DatabasePrivilegeInsert, auth.DatabasePrivilegeUpdate},
					},
				},
				CreatedAt: time.Now().UTC(),
				UpdatedAt: time.Now().UTC(),
			}

			err := storage.Store(accessKey)

			if err != nil {
				t.Fatalf("Failed to store complex access key: %v", err)
			}

			retrievedKey, err := storage.Get("complex_access_key_id")

			if err != nil {
				t.Fatalf("Failed to get complex access key: %v", err)
			}

			// Verify all statements are preserved
			if len(retrievedKey.Statements) != 3 {
				t.Fatalf("Expected 3 statements, got %d", len(retrievedKey.Statements))
			}

			// Check first statement
			stmt := retrievedKey.Statements[0]

			if stmt.Effect != auth.StatementEffectAllow {
				t.Errorf("Expected first statement effect Allow, got %s", stmt.Effect)
			}

			if stmt.Resource != "database:*" {
				t.Errorf("Expected first statement resource 'database:*', got %s", stmt.Resource)
			}

			if len(stmt.Actions) != 2 {
				t.Errorf("Expected first statement to have 2 actions, got %d", len(stmt.Actions))
			}

			stmt = retrievedKey.Statements[1]

			if stmt.Effect != auth.StatementEffectDeny {
				t.Errorf("Expected second statement effect Deny, got %s", stmt.Effect)
			}

			if stmt.Resource != "database:sensitive:*" {
				t.Errorf("Expected second statement resource 'database:sensitive:*', got %s", stmt.Resource)
			}

			stmt = retrievedKey.Statements[2]

			if stmt.Effect != auth.StatementEffectAllow {
				t.Errorf("Expected third statement effect Allow, got %s", stmt.Effect)
			}

			if stmt.Resource != "database:public:table:users" {
				t.Errorf("Expected third statement resource 'database:public:table:users', got %s", stmt.Resource)
			}
		})

		t.Run("Store with empty description", func(t *testing.T) {
			accessKey := &auth.AccessKey{
				AccessKeyID:     "empty_desc_key",
				AccessKeySecret: "test_secret",
				Description:     "",
				Statements: []auth.AccessKeyStatement{
					{
						Effect:   auth.StatementEffectAllow,
						Resource: "*",
						Actions:  []auth.Privilege{"*"},
					},
				},
				CreatedAt: time.Now().UTC(),
				UpdatedAt: time.Now().UTC(),
			}

			err := storage.Store(accessKey)

			if err != nil {
				t.Fatalf("Failed to store access key with empty description: %v", err)
			}

			retrievedKey, err := storage.Get("empty_desc_key")

			if err != nil {
				t.Fatalf("Failed to get access key with empty description: %v", err)
			}

			if retrievedKey.Description != "" {
				t.Errorf("Expected empty description, got %s", retrievedKey.Description)
			}
		})

		t.Run("Store with no statements", func(t *testing.T) {
			accessKey := &auth.AccessKey{
				AccessKeyID:     "no_statements_key",
				AccessKeySecret: "test_secret",
				Description:     "Key with no statements",
				Statements:      []auth.AccessKeyStatement{},
				CreatedAt:       time.Now().UTC(),
				UpdatedAt:       time.Now().UTC(),
			}

			err := storage.Store(accessKey)

			if err != nil {
				t.Fatalf("Failed to store access key with no statements: %v", err)
			}

			retrievedKey, err := storage.Get("no_statements_key")

			if err != nil {
				t.Fatalf("Failed to get access key with no statements: %v", err)
			}

			if len(retrievedKey.Statements) != 0 {
				t.Errorf("Expected 0 statements, got %d", len(retrievedKey.Statements))
			}
		})

		t.Run("Secret encryption/decryption", func(t *testing.T) {
			originalSecret := "very_secret_access_key_12345!@#$%"

			accessKey := &auth.AccessKey{
				AccessKeyID:     "encryption_test_key",
				AccessKeySecret: originalSecret,
				Description:     "Test encryption",
				Statements: []auth.AccessKeyStatement{
					{
						Effect:   auth.StatementEffectAllow,
						Resource: "*",
						Actions:  []auth.Privilege{"*"},
					},
				},
				CreatedAt: time.Now().UTC(),
				UpdatedAt: time.Now().UTC(),
			}

			err := storage.Store(accessKey)

			if err != nil {
				t.Fatalf("Failed to store access key for encryption test: %v", err)
			}

			db, err := app.DatabaseManager.SystemDatabase().DB()

			if err != nil {
				t.Fatalf("Failed to get system database: %v", err)
			}

			var storedSecret string

			err = db.QueryRow("SELECT access_key_secret FROM access_keys WHERE access_key_id = ?", "encryption_test_key").
				Scan(&storedSecret)

			if err != nil {
				t.Fatalf("Failed to query stored secret: %v", err)
			}

			if storedSecret == originalSecret {
				t.Error("Secret appears to be stored in plain text instead of encrypted")
			}

			retrievedKey, err := storage.Get("encryption_test_key")

			if err != nil {
				t.Fatalf("Failed to get access key for encryption test: %v", err)
			}

			if retrievedKey.AccessKeySecret != originalSecret {
				t.Errorf("Expected decrypted secret %s, got %s", originalSecret, retrievedKey.AccessKeySecret)
			}
		})

		t.Run("List empty storage", func(t *testing.T) {
			keys, err := storage.List()

			if err != nil {
				t.Fatalf("Failed to list access keys: %v", err)
			}

			if keys == nil {
				t.Error("Expected List to return empty slice, got nil")
			}

			initialCount := len(keys)

			t.Logf("Initial key count: %d", initialCount)
		})

		t.Run("List with single key", func(t *testing.T) {
			accessKey := &auth.AccessKey{
				AccessKeyID:     "list_test_single_key",
				AccessKeySecret: "list_test_secret",
				Description:     "Single key for list test",
				Statements: []auth.AccessKeyStatement{
					{
						Effect:   auth.StatementEffectAllow,
						Resource: "database:*",
						Actions:  []auth.Privilege{auth.DatabasePrivilegeRead},
					},
				},
				CreatedAt: time.Now().UTC(),
				UpdatedAt: time.Now().UTC(),
			}

			err := storage.Store(accessKey)

			if err != nil {
				t.Fatalf("Failed to store access key for list test: %v", err)
			}

			keys, err := storage.List()

			if err != nil {
				t.Fatalf("Failed to list access keys: %v", err)
			}

			var foundKey *auth.AccessKey

			for _, key := range keys {
				if key.AccessKeyID == "list_test_single_key" {
					foundKey = key
					break
				}
			}

			if foundKey == nil {
				t.Error("Expected to find our test key in the list, but didn't")
			} else {
				if foundKey.AccessKeySecret != accessKey.AccessKeySecret {
					t.Errorf("Expected AccessKeySecret %s, got %s", accessKey.AccessKeySecret, foundKey.AccessKeySecret)
				}

				if foundKey.Description != accessKey.Description {
					t.Errorf("Expected Description %s, got %s", accessKey.Description, foundKey.Description)
				}

				if len(foundKey.Statements) != len(accessKey.Statements) {
					t.Errorf("Expected %d statements, got %d", len(accessKey.Statements), len(foundKey.Statements))
				}
			}

			err = storage.Delete("list_test_single_key")

			if err != nil {
				t.Fatalf("Failed to clean up test key: %v", err)
			}
		})

		t.Run("List with multiple keys", func(t *testing.T) {
			testKeys := []*auth.AccessKey{
				{
					AccessKeyID:     "list_test_key_1",
					AccessKeySecret: "secret_1",
					Description:     "First test key",
					Statements: []auth.AccessKeyStatement{
						{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
					},
					CreatedAt: time.Now().UTC(),
					UpdatedAt: time.Now().UTC(),
				},
				{
					AccessKeyID:     "list_test_key_2",
					AccessKeySecret: "secret_2",
					Description:     "Second test key",
					Statements: []auth.AccessKeyStatement{
						{Effect: auth.StatementEffectDeny, Resource: "database:sensitive", Actions: []auth.Privilege{auth.DatabasePrivilegeDelete}},
					},
					CreatedAt: time.Now().UTC().Add(time.Minute),
					UpdatedAt: time.Now().UTC().Add(time.Minute),
				},
				{
					AccessKeyID:     "list_test_key_3",
					AccessKeySecret: "secret_3",
					Description:     "Third test key",
					Statements: []auth.AccessKeyStatement{
						{Effect: auth.StatementEffectAllow, Resource: "database:public:table:users", Actions: []auth.Privilege{auth.DatabasePrivilegeRead, auth.DatabasePrivilegeInsert}},
					},
					CreatedAt: time.Now().UTC().Add(2 * time.Minute),
					UpdatedAt: time.Now().UTC().Add(2 * time.Minute),
				},
			}

			for _, key := range testKeys {
				err := storage.Store(key)

				if err != nil {
					t.Fatalf("Failed to store test key %s: %v", key.AccessKeyID, err)
				}
			}

			keys, err := storage.List()

			if err != nil {
				t.Fatalf("Failed to list access keys: %v", err)
			}

			foundKeys := make(map[string]*auth.AccessKey)

			for _, key := range keys {
				if strings.Contains(key.AccessKeyID, "list_test_key_") {
					foundKeys[key.AccessKeyID] = key
				}
			}

			if len(foundKeys) != 3 {
				t.Errorf("Expected to find 3 test keys, found %d", len(foundKeys))
			}

			for _, originalKey := range testKeys {
				foundKey, exists := foundKeys[originalKey.AccessKeyID]

				if !exists {
					t.Errorf("Expected to find key %s in list", originalKey.AccessKeyID)
					continue
				}

				if foundKey.AccessKeySecret != originalKey.AccessKeySecret {
					t.Errorf("Key %s: Expected AccessKeySecret %s, got %s", originalKey.AccessKeyID, originalKey.AccessKeySecret, foundKey.AccessKeySecret)
				}

				if foundKey.Description != originalKey.Description {
					t.Errorf("Key %s: Expected Description %s, got %s", originalKey.AccessKeyID, originalKey.Description, foundKey.Description)
				}
			}

			var previousCreatedAt time.Time
			testKeyTimestamps := make(map[string]time.Time)

			for _, key := range testKeys {
				testKeyTimestamps[key.AccessKeyID] = key.CreatedAt
			}

			for _, key := range keys {
				if strings.Contains(key.AccessKeyID, "list_test_key_") {
					if !previousCreatedAt.IsZero() {
						expectedTime := testKeyTimestamps[key.AccessKeyID]

						if key.CreatedAt.Sub(expectedTime).Abs() > time.Second {
							t.Errorf("Unexpected timestamp for key %s: expected around %v, got %v", key.AccessKeyID, expectedTime, key.CreatedAt)
						}
					}

					previousCreatedAt = key.CreatedAt
				}
			}

			for _, key := range testKeys {
				err := storage.Delete(key.AccessKeyID)

				if err != nil {
					t.Fatalf("Failed to clean up test key %s: %v", key.AccessKeyID, err)
				}
			}
		})

		t.Run("List with complex statements", func(t *testing.T) {
			complexKey := &auth.AccessKey{
				AccessKeyID:     "list_test_complex_key",
				AccessKeySecret: "complex_secret",
				Description:     "Key with complex statements",
				Statements: []auth.AccessKeyStatement{
					{
						Effect:   auth.StatementEffectAllow,
						Resource: "database:app:branch:main:table:users",
						Actions:  []auth.Privilege{auth.DatabasePrivilegeSelect, auth.DatabasePrivilegeInsert},
					},
					{
						Effect:   auth.StatementEffectDeny,
						Resource: "database:app:branch:main:table:users:column:password",
						Actions:  []auth.Privilege{auth.DatabasePrivilegeSelect},
					},
					{
						Effect:   auth.StatementEffectAllow,
						Resource: "database:*:branch:development:*",
						Actions:  []auth.Privilege{"*"},
					},
				},
				CreatedAt: time.Now().UTC(),
				UpdatedAt: time.Now().UTC(),
			}

			err := storage.Store(complexKey)

			if err != nil {
				t.Fatalf("Failed to store complex key: %v", err)
			}

			keys, err := storage.List()

			if err != nil {
				t.Fatalf("Failed to list access keys: %v", err)
			}

			var foundKey *auth.AccessKey

			for _, key := range keys {
				if key.AccessKeyID == "list_test_complex_key" {
					foundKey = key
					break
				}
			}

			if foundKey == nil {
				t.Fatal("Expected to find complex test key in list")
			}

			if len(foundKey.Statements) != 3 {
				t.Fatalf("Expected 3 statements, got %d", len(foundKey.Statements))
			}

			stmt := foundKey.Statements[0]
			if stmt.Effect != auth.StatementEffectAllow {
				t.Errorf("Expected first statement effect Allow, got %s", stmt.Effect)
			}

			if stmt.Resource != "database:app:branch:main:table:users" {
				t.Errorf("Expected first statement resource 'database:app:branch:main:table:users', got %s", stmt.Resource)
			}

			if len(stmt.Actions) != 2 {
				t.Errorf("Expected first statement to have 2 actions, got %d", len(stmt.Actions))
			}

			err = storage.Delete("list_test_complex_key")

			if err != nil {
				t.Fatalf("Failed to clean up complex test key: %v", err)
			}
		})

		t.Run("Delete existing key", func(t *testing.T) {
			accessKey := &auth.AccessKey{
				AccessKeyID:     "delete_test_key",
				AccessKeySecret: "delete_test_secret",
				Description:     "Key to be deleted",
				Statements: []auth.AccessKeyStatement{
					{
						Effect:   auth.StatementEffectAllow,
						Resource: "*",
						Actions:  []auth.Privilege{"*"},
					},
				},
				CreatedAt: time.Now().UTC(),
				UpdatedAt: time.Now().UTC(),
			}

			err := storage.Store(accessKey)

			if err != nil {
				t.Fatalf("Failed to store access key for delete test: %v", err)
			}

			_, err = storage.Get("delete_test_key")

			if err != nil {
				t.Fatalf("Failed to get access key before deletion: %v", err)
			}

			err = storage.Delete("delete_test_key")

			if err != nil {
				t.Fatalf("Failed to delete access key: %v", err)
			}

			_, err = storage.Get("delete_test_key")

			if err == nil {
				t.Error("Expected error when getting deleted key, but got nil")
			}
		})

		t.Run("Delete non-existent key", func(t *testing.T) {
			err := storage.Delete("non_existent_key_for_delete")

			if err == nil {
				t.Error("Expected error when deleting non-existent key, but got nil")
			}

			expectedErrorSubstring := "not found"

			if !strings.Contains(err.Error(), expectedErrorSubstring) {
				t.Errorf("Expected error to contain '%s', got: %v", expectedErrorSubstring, err)
			}
		})

		t.Run("Delete and verify database state", func(t *testing.T) {
			keys := []*auth.AccessKey{
				{
					AccessKeyID:     "multi_delete_key_1",
					AccessKeySecret: "secret1",
					Description:     "First key",
					Statements: []auth.AccessKeyStatement{
						{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
					},
					CreatedAt: time.Now().UTC(),
					UpdatedAt: time.Now().UTC(),
				},
				{
					AccessKeyID:     "multi_delete_key_2",
					AccessKeySecret: "secret2",
					Description:     "Second key",
					Statements: []auth.AccessKeyStatement{
						{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
					},
					CreatedAt: time.Now().UTC(),
					UpdatedAt: time.Now().UTC(),
				},
			}

			for _, key := range keys {
				err := storage.Store(key)

				if err != nil {
					t.Fatalf("Failed to store access key %s: %v", key.AccessKeyID, err)
				}
			}

			for _, key := range keys {
				_, err := storage.Get(key.AccessKeyID)

				if err != nil {
					t.Fatalf("Failed to get access key %s: %v", key.AccessKeyID, err)
				}
			}

			err := storage.Delete("multi_delete_key_1")

			if err != nil {
				t.Fatalf("Failed to delete first key: %v", err)
			}

			_, err = storage.Get("multi_delete_key_1")

			if err == nil {
				t.Error("Expected error when getting deleted key, but got nil")
			}

			_, err = storage.Get("multi_delete_key_2")

			if err != nil {
				t.Errorf("Second key should still exist after deleting first key: %v", err)
			}

			err = storage.Delete("multi_delete_key_2")

			if err != nil {
				t.Fatalf("Failed to clean up second key: %v", err)
			}
		})

		t.Run("Delete with special characters in ID", func(t *testing.T) {
			specialID := "test'key\"with;special--chars"

			accessKey := &auth.AccessKey{
				AccessKeyID:     specialID,
				AccessKeySecret: "special_secret",
				Description:     "Key with special characters",
				Statements: []auth.AccessKeyStatement{
					{
						Effect:   auth.StatementEffectAllow,
						Resource: "*",
						Actions:  []auth.Privilege{"*"},
					},
				},
				CreatedAt: time.Now().UTC(),
				UpdatedAt: time.Now().UTC(),
			}

			err := storage.Store(accessKey)

			if err != nil {
				t.Fatalf("Failed to store access key with special characters: %v", err)
			}

			_, err = storage.Get(specialID)

			if err != nil {
				t.Fatalf("Failed to get access key with special characters: %v", err)
			}

			err = storage.Delete(specialID)

			if err != nil {
				t.Fatalf("Failed to delete access key with special characters: %v", err)
			}

			_, err = storage.Get(specialID)

			if err == nil {
				t.Error("Expected error when getting deleted key with special characters, but got nil")
			}
		})

		t.Run("Delete with empty ID", func(t *testing.T) {
			err := storage.Delete("")

			if err == nil {
				t.Error("Expected error when deleting with empty ID, but got nil")
			}

			expectedErrorSubstring := "not found"

			if !strings.Contains(err.Error(), expectedErrorSubstring) {
				t.Errorf("Expected error to contain '%s', got: %v", expectedErrorSubstring, err)
			}
		})

		t.Run("Update", func(t *testing.T) {
			t.Run("Update existing key", func(t *testing.T) {
				originalKey := &auth.AccessKey{
					AccessKeyID:     "test-update-key-1",
					AccessKeySecret: "original-secret",
					Description:     "Original description",
					Statements: []auth.AccessKeyStatement{
						{
							Effect:   auth.StatementEffectAllow,
							Resource: "database:original",
							Actions:  []auth.Privilege{auth.DatabasePrivilegeRead},
						},
					},
					CreatedAt: time.Now().UTC().Truncate(time.Second),
					UpdatedAt: time.Now().UTC().Truncate(time.Second),
				}

				err := storage.Store(originalKey)

				if err != nil {
					t.Fatalf("Failed to store original key: %v", err)
				}

				updatedKey := &auth.AccessKey{
					AccessKeyID:     "test-update-key-1",
					AccessKeySecret: "updated-secret",
					Description:     "Updated description",
					Statements: []auth.AccessKeyStatement{
						{
							Effect:   auth.StatementEffectAllow,
							Resource: "database:updated",
							Actions:  []auth.Privilege{auth.DatabasePrivilegeUpdate},
						},
						{
							Effect:   auth.StatementEffectDeny,
							Resource: "database:restricted",
							Actions:  []auth.Privilege{"*"},
						},
					},
					CreatedAt: originalKey.CreatedAt,           // Should remain the same
					UpdatedAt: time.Now().UTC().Add(time.Hour), // Should be updated
				}

				err = storage.Update(updatedKey)

				if err != nil {
					t.Fatalf("Failed to update key: %v", err)
				}

				retrievedKey, err := storage.Get("test-update-key-1")

				if err != nil {
					t.Fatalf("Failed to get updated key: %v", err)
				}

				if retrievedKey.AccessKeySecret != "updated-secret" {
					t.Errorf("Expected AccessKeySecret 'updated-secret', got '%s'", retrievedKey.AccessKeySecret)
				}

				if retrievedKey.Description != "Updated description" {
					t.Errorf("Expected Description 'Updated description', got '%s'", retrievedKey.Description)
				}

				if len(retrievedKey.Statements) != 2 {
					t.Fatalf("Expected 2 statements, got %d", len(retrievedKey.Statements))
				}

				if retrievedKey.Statements[0].Resource != "database:updated" {
					t.Errorf("Expected first statement resource 'database:updated', got '%s'", retrievedKey.Statements[0].Resource)
				}

				if retrievedKey.Statements[0].Actions[0] != auth.DatabasePrivilegeUpdate {
					t.Errorf("Expected first statement action 'database:update', got '%s'", retrievedKey.Statements[0].Actions[0])
				}

				if retrievedKey.Statements[1].Effect != auth.StatementEffectDeny {
					t.Errorf("Expected second statement effect 'Deny', got '%s'", retrievedKey.Statements[1].Effect)
				}

				if !retrievedKey.CreatedAt.Equal(originalKey.CreatedAt) {
					t.Errorf("CreatedAt should not change on update. Expected %v, got %v", originalKey.CreatedAt, retrievedKey.CreatedAt)
				}

				if !retrievedKey.UpdatedAt.After(originalKey.UpdatedAt) {
					t.Errorf("UpdatedAt should be newer than original. Original: %v, Updated: %v", originalKey.UpdatedAt, retrievedKey.UpdatedAt)
				}
			})

			t.Run("Update non-existent key", func(t *testing.T) {
				nonExistentKey := &auth.AccessKey{
					AccessKeyID:     "non-existent-key",
					AccessKeySecret: "secret",
					Description:     "Description",
					Statements:      []auth.AccessKeyStatement{},
					CreatedAt:       time.Now().UTC(),
					UpdatedAt:       time.Now().UTC(),
				}

				err := storage.Update(nonExistentKey)

				if err == nil {
					t.Error("Expected error when updating non-existent key, but got nil")
				}

				expectedErrorSubstring := "not found"

				if !strings.Contains(err.Error(), expectedErrorSubstring) {
					t.Errorf("Expected error to contain '%s', got: %v", expectedErrorSubstring, err)
				}
			})

			t.Run("Update with empty statements", func(t *testing.T) {
				originalKey := &auth.AccessKey{
					AccessKeyID:     "test-update-empty-statements",
					AccessKeySecret: "secret",
					Description:     "Description",
					Statements: []auth.AccessKeyStatement{
						{
							Effect:   auth.StatementEffectAllow,
							Resource: "database:test",
							Actions:  []auth.Privilege{auth.DatabasePrivilegeRead},
						},
					},
					CreatedAt: time.Now().UTC(),
					UpdatedAt: time.Now().UTC(),
				}

				err := storage.Store(originalKey)

				if err != nil {
					t.Fatalf("Failed to store original key: %v", err)
				}

				updatedKey := &auth.AccessKey{
					AccessKeyID:     "test-update-empty-statements",
					AccessKeySecret: "updated-secret",
					Description:     "Updated with empty statements",
					Statements:      []auth.AccessKeyStatement{}, // Empty statements
					CreatedAt:       originalKey.CreatedAt,
					UpdatedAt:       time.Now().UTC().Add(time.Hour),
				}

				err = storage.Update(updatedKey)

				if err != nil {
					t.Fatalf("Failed to update key with empty statements: %v", err)
				}

				retrievedKey, err := storage.Get("test-update-empty-statements")

				if err != nil {
					t.Fatalf("Failed to get updated key: %v", err)
				}

				if len(retrievedKey.Statements) != 0 {
					t.Errorf("Expected 0 statements, got %d", len(retrievedKey.Statements))
				}

				if retrievedKey.Description != "Updated with empty statements" {
					t.Errorf("Expected Description 'Updated with empty statements', got '%s'", retrievedKey.Description)
				}
			})

			t.Run("Update with complex statements", func(t *testing.T) {
				originalKey := &auth.AccessKey{
					AccessKeyID:     "test-update-complex",
					AccessKeySecret: "secret",
					Description:     "Complex update test",
					Statements: []auth.AccessKeyStatement{
						{
							Effect:   auth.StatementEffectAllow,
							Resource: "database:simple",
							Actions:  []auth.Privilege{auth.DatabasePrivilegeRead},
						},
					},
					CreatedAt: time.Now().UTC(),
					UpdatedAt: time.Now().UTC(),
				}

				err := storage.Store(originalKey)

				if err != nil {
					t.Fatalf("Failed to store original key: %v", err)
				}

				complexStatements := []auth.AccessKeyStatement{
					{
						Effect:   auth.StatementEffectAllow,
						Resource: "database:production/*",
						Actions:  []auth.Privilege{auth.DatabasePrivilegeRead, auth.DatabasePrivilegeSelect},
					},
					{
						Effect:   auth.StatementEffectDeny,
						Resource: "database:sensitive/*",
						Actions:  []auth.Privilege{"*"},
					},
					{
						Effect:   auth.StatementEffectAllow,
						Resource: "database:public/*",
						Actions:  []auth.Privilege{auth.DatabasePrivilegeRead, auth.DatabasePrivilegeUpdate},
					},
				}

				updatedKey := &auth.AccessKey{
					AccessKeyID:     "test-update-complex",
					AccessKeySecret: "updated-complex-secret",
					Description:     "Updated with complex statements",
					Statements:      complexStatements,
					CreatedAt:       originalKey.CreatedAt,
					UpdatedAt:       time.Now().UTC().Add(time.Hour),
				}

				err = storage.Update(updatedKey)

				if err != nil {
					t.Fatalf("Failed to update key with complex statements: %v", err)
				}

				retrievedKey, err := storage.Get("test-update-complex")

				if err != nil {
					t.Fatalf("Failed to get updated key: %v", err)
				}

				if len(retrievedKey.Statements) != 3 {
					t.Fatalf("Expected 3 statements, got %d", len(retrievedKey.Statements))
				}

				stmt1 := retrievedKey.Statements[0]

				if stmt1.Effect != auth.StatementEffectAllow {
					t.Errorf("Expected first statement effect 'Allow', got '%s'", stmt1.Effect)
				}

				if stmt1.Resource != "database:production/*" {
					t.Errorf("Expected first statement resource 'database:production/*', got '%s'", stmt1.Resource)
				}

				if len(stmt1.Actions) != 2 {
					t.Fatalf("Expected first statement to have 2 actions, got %d", len(stmt1.Actions))
				}

				stmt2 := retrievedKey.Statements[1]

				if stmt2.Effect != auth.StatementEffectDeny {
					t.Errorf("Expected second statement effect 'Deny', got '%s'", stmt2.Effect)
				}

				if stmt2.Actions[0] != "*" {
					t.Errorf("Expected second statement action '*', got '%s'", stmt2.Actions[0])
				}

				stmt3 := retrievedKey.Statements[2]

				if stmt3.Resource != "database:public/*" {
					t.Errorf("Expected third statement resource 'database:public/*', got '%s'", stmt3.Resource)
				}

				if len(stmt3.Actions) != 2 {
					t.Fatalf("Expected third statement to have 2 actions, got %d", len(stmt3.Actions))
				}
			})
		})
	})
}
