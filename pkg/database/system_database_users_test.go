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

func TestSystemDatabaseUsers(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		// Create a test user storage instance
		storage := database.NewSystemDatabaseUserStorage(
			app.DatabaseManager.SystemDatabase(),
		)

		t.Run("Store and Get", func(t *testing.T) {
			user := &auth.User{
				Username:    "test_user",
				Password:    "test_password_hash",
				Description: "Test user",
				Statements: []auth.AccessKeyStatement{
					{
						Effect:   auth.AccessKeyEffectAllow,
						Resource: "*",
						Actions:  []auth.Privilege{"*"},
					},
				},
				CreatedAt: time.Now().UTC(),
				UpdatedAt: time.Now().UTC(),
			}

			err := storage.Store(user)

			if err != nil {
				t.Fatalf("Failed to store user: %v", err)
			}

			// Test Get
			retrievedUser, err := storage.Get("test_user")

			if err != nil {
				t.Fatalf("Failed to get user: %v", err)
			}

			if retrievedUser.Username != user.Username {
				t.Errorf("Expected Username %s, got %s", user.Username, retrievedUser.Username)
			}

			if retrievedUser.Password != user.Password {
				t.Errorf("Expected Password %s, got %s", user.Password, retrievedUser.Password)
			}

			if retrievedUser.Description != user.Description {
				t.Errorf("Expected Description %s, got %s", user.Description, retrievedUser.Description)
			}

			if len(retrievedUser.Statements) != len(user.Statements) {
				t.Errorf("Expected %d statements, got %d", len(user.Statements), len(retrievedUser.Statements))
			} else {
				if retrievedUser.Statements[0].Effect != user.Statements[0].Effect {
					t.Errorf("Expected Effect %s, got %s", user.Statements[0].Effect, retrievedUser.Statements[0].Effect)
				}

				if retrievedUser.Statements[0].Resource != user.Statements[0].Resource {
					t.Errorf("Expected Resource %s, got %s", user.Statements[0].Resource, retrievedUser.Statements[0].Resource)
				}

				if len(retrievedUser.Statements[0].Actions) != len(user.Statements[0].Actions) {
					t.Errorf("Expected %d actions, got %d", len(user.Statements[0].Actions), len(retrievedUser.Statements[0].Actions))
				}
			}

			if retrievedUser.CreatedAt.Sub(user.CreatedAt).Abs() > time.Second {
				t.Errorf("CreatedAt timestamp differs too much: expected %v, got %v", user.CreatedAt, retrievedUser.CreatedAt)
			}

			if retrievedUser.UpdatedAt.Sub(user.UpdatedAt).Abs() > time.Second {
				t.Errorf("UpdatedAt timestamp differs too much: expected %v, got %v", user.UpdatedAt, retrievedUser.UpdatedAt)
			}
		})

		t.Run("Get non-existent user", func(t *testing.T) {
			_, err := storage.Get("non_existent_user")

			if err == nil {
				t.Error("Expected error when getting non-existent user, but got nil")
			}
		})

		t.Run("Store with complex statements", func(t *testing.T) {
			user := &auth.User{
				Username:    "complex_user",
				Password:    "complex_password_hash",
				Description: "Complex user with multiple statements",
				Statements: []auth.AccessKeyStatement{
					{
						Effect:   auth.AccessKeyEffectAllow,
						Resource: "database:*",
						Actions:  []auth.Privilege{auth.DatabasePrivilegeRead, auth.DatabasePrivilegeSelect},
					},
					{
						Effect:   auth.AccessKeyEffectDeny,
						Resource: "database:sensitive:*",
						Actions:  []auth.Privilege{auth.DatabasePrivilegeDelete},
					},
					{
						Effect:   auth.AccessKeyEffectAllow,
						Resource: "database:public:table:users",
						Actions:  []auth.Privilege{auth.DatabasePrivilegeInsert, auth.DatabasePrivilegeUpdate},
					},
				},
				CreatedAt: time.Now().UTC(),
				UpdatedAt: time.Now().UTC(),
			}

			err := storage.Store(user)

			if err != nil {
				t.Fatalf("Failed to store complex user: %v", err)
			}

			retrievedUser, err := storage.Get("complex_user")

			if err != nil {
				t.Fatalf("Failed to get complex user: %v", err)
			}

			// Verify all statements are preserved
			if len(retrievedUser.Statements) != 3 {
				t.Fatalf("Expected 3 statements, got %d", len(retrievedUser.Statements))
			}

			// Check first statement
			stmt := retrievedUser.Statements[0]

			if stmt.Effect != auth.AccessKeyEffectAllow {
				t.Errorf("Expected first statement effect Allow, got %s", stmt.Effect)
			}

			if stmt.Resource != "database:*" {
				t.Errorf("Expected first statement resource 'database:*', got %s", stmt.Resource)
			}

			if len(stmt.Actions) != 2 {
				t.Errorf("Expected first statement to have 2 actions, got %d", len(stmt.Actions))
			}

			stmt = retrievedUser.Statements[1]

			if stmt.Effect != auth.AccessKeyEffectDeny {
				t.Errorf("Expected second statement effect Deny, got %s", stmt.Effect)
			}

			if stmt.Resource != "database:sensitive:*" {
				t.Errorf("Expected second statement resource 'database:sensitive:*', got %s", stmt.Resource)
			}

			stmt = retrievedUser.Statements[2]

			if stmt.Effect != auth.AccessKeyEffectAllow {
				t.Errorf("Expected third statement effect Allow, got %s", stmt.Effect)
			}

			if stmt.Resource != "database:public:table:users" {
				t.Errorf("Expected third statement resource 'database:public:table:users', got %s", stmt.Resource)
			}
		})

		t.Run("Store with empty description", func(t *testing.T) {
			user := &auth.User{
				Username:    "empty_desc_user",
				Password:    "test_password",
				Description: "",
				Statements: []auth.AccessKeyStatement{
					{
						Effect:   auth.AccessKeyEffectAllow,
						Resource: "*",
						Actions:  []auth.Privilege{"*"},
					},
				},
				CreatedAt: time.Now().UTC(),
				UpdatedAt: time.Now().UTC(),
			}

			err := storage.Store(user)

			if err != nil {
				t.Fatalf("Failed to store user with empty description: %v", err)
			}

			retrievedUser, err := storage.Get("empty_desc_user")

			if err != nil {
				t.Fatalf("Failed to get user with empty description: %v", err)
			}

			if retrievedUser.Description != "" {
				t.Errorf("Expected empty description, got %s", retrievedUser.Description)
			}
		})

		t.Run("Store with no statements", func(t *testing.T) {
			user := &auth.User{
				Username:    "no_statements_user",
				Password:    "test_password",
				Description: "User with no statements",
				Statements:  []auth.AccessKeyStatement{},
				CreatedAt:   time.Now().UTC(),
				UpdatedAt:   time.Now().UTC(),
			}

			err := storage.Store(user)

			if err != nil {
				t.Fatalf("Failed to store user with no statements: %v", err)
			}

			retrievedUser, err := storage.Get("no_statements_user")

			if err != nil {
				t.Fatalf("Failed to get user with no statements: %v", err)
			}

			if len(retrievedUser.Statements) != 0 {
				t.Errorf("Expected 0 statements, got %d", len(retrievedUser.Statements))
			}
		})

		t.Run("Password storage", func(t *testing.T) {
			originalPassword := "very_secret_password_12345!@#$%"

			user := &auth.User{
				Username:    "password_test_user",
				Password:    originalPassword,
				Description: "Test password storage",
				Statements: []auth.AccessKeyStatement{
					{
						Effect:   auth.AccessKeyEffectAllow,
						Resource: "*",
						Actions:  []auth.Privilege{"*"},
					},
				},
				CreatedAt: time.Now().UTC(),
				UpdatedAt: time.Now().UTC(),
			}

			err := storage.Store(user)

			if err != nil {
				t.Fatalf("Failed to store user for password test: %v", err)
			}

			db, err := app.DatabaseManager.SystemDatabase().DB()

			if err != nil {
				t.Fatalf("Failed to get system database: %v", err)
			}

			var storedPassword string

			err = db.QueryRow("SELECT password FROM users WHERE username = ?", "password_test_user").
				Scan(&storedPassword)

			if err != nil {
				t.Fatalf("Failed to query stored password: %v", err)
			}

			if storedPassword != originalPassword {
				t.Errorf("Expected stored password %s, got %s", originalPassword, storedPassword)
			}

			retrievedUser, err := storage.Get("password_test_user")

			if err != nil {
				t.Fatalf("Failed to get user for password test: %v", err)
			}

			if retrievedUser.Password != originalPassword {
				t.Errorf("Expected retrieved password %s, got %s", originalPassword, retrievedUser.Password)
			}
		})

		t.Run("List empty storage", func(t *testing.T) {
			users, err := storage.List()

			if err != nil {
				t.Fatalf("Failed to list users: %v", err)
			}

			if users == nil {
				t.Error("Expected List to return empty slice, got nil")
			}

			initialCount := len(users)

			t.Logf("Initial user count: %d", initialCount)
		})

		t.Run("List with single user", func(t *testing.T) {
			user := &auth.User{
				Username:    "list_test_single_user",
				Password:    "list_test_password",
				Description: "Single user for list test",
				Statements: []auth.AccessKeyStatement{
					{
						Effect:   auth.AccessKeyEffectAllow,
						Resource: "database:*",
						Actions:  []auth.Privilege{auth.DatabasePrivilegeRead},
					},
				},
				CreatedAt: time.Now().UTC(),
				UpdatedAt: time.Now().UTC(),
			}

			err := storage.Store(user)

			if err != nil {
				t.Fatalf("Failed to store user for list test: %v", err)
			}

			users, err := storage.List()

			if err != nil {
				t.Fatalf("Failed to list users: %v", err)
			}

			var foundUser *auth.User

			for _, u := range users {
				if u.Username == "list_test_single_user" {
					foundUser = u
					break
				}
			}

			if foundUser == nil {
				t.Error("Expected to find our test user in the list, but didn't")
			} else {
				if foundUser.Password != user.Password {
					t.Errorf("Expected Password %s, got %s", user.Password, foundUser.Password)
				}

				if foundUser.Description != user.Description {
					t.Errorf("Expected Description %s, got %s", user.Description, foundUser.Description)
				}

				if len(foundUser.Statements) != len(user.Statements) {
					t.Errorf("Expected %d statements, got %d", len(user.Statements), len(foundUser.Statements))
				}
			}

			err = storage.Delete("list_test_single_user")

			if err != nil {
				t.Fatalf("Failed to clean up test user: %v", err)
			}
		})

		t.Run("List with multiple users", func(t *testing.T) {
			testUsers := []*auth.User{
				{
					Username:    "list_test_user_1",
					Password:    "password_1",
					Description: "First test user",
					Statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
					},
					CreatedAt: time.Now().UTC(),
					UpdatedAt: time.Now().UTC(),
				},
				{
					Username:    "list_test_user_2",
					Password:    "password_2",
					Description: "Second test user",
					Statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectDeny, Resource: "database:sensitive", Actions: []auth.Privilege{auth.DatabasePrivilegeDelete}},
					},
					CreatedAt: time.Now().UTC().Add(time.Minute),
					UpdatedAt: time.Now().UTC().Add(time.Minute),
				},
				{
					Username:    "list_test_user_3",
					Password:    "password_3",
					Description: "Third test user",
					Statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "database:public:table:users", Actions: []auth.Privilege{auth.DatabasePrivilegeRead, auth.DatabasePrivilegeInsert}},
					},
					CreatedAt: time.Now().UTC().Add(2 * time.Minute),
					UpdatedAt: time.Now().UTC().Add(2 * time.Minute),
				},
			}

			for _, user := range testUsers {
				err := storage.Store(user)

				if err != nil {
					t.Fatalf("Failed to store test user %s: %v", user.Username, err)
				}
			}

			users, err := storage.List()

			if err != nil {
				t.Fatalf("Failed to list users: %v", err)
			}

			foundUsers := make(map[string]*auth.User)

			for _, user := range users {
				if strings.Contains(user.Username, "list_test_user_") {
					foundUsers[user.Username] = user
				}
			}

			if len(foundUsers) != 3 {
				t.Errorf("Expected to find 3 test users, found %d", len(foundUsers))
			}

			for _, originalUser := range testUsers {
				foundUser, exists := foundUsers[originalUser.Username]

				if !exists {
					t.Errorf("Expected to find user %s in list", originalUser.Username)
					continue
				}

				if foundUser.Password != originalUser.Password {
					t.Errorf("User %s: Expected Password %s, got %s", originalUser.Username, originalUser.Password, foundUser.Password)
				}

				if foundUser.Description != originalUser.Description {
					t.Errorf("User %s: Expected Description %s, got %s", originalUser.Username, originalUser.Description, foundUser.Description)
				}
			}

			var previousCreatedAt time.Time
			testUserTimestamps := make(map[string]time.Time)

			for _, user := range testUsers {
				testUserTimestamps[user.Username] = user.CreatedAt
			}

			for _, user := range users {
				if strings.Contains(user.Username, "list_test_user_") {
					if !previousCreatedAt.IsZero() {
						expectedTime := testUserTimestamps[user.Username]

						if user.CreatedAt.Sub(expectedTime).Abs() > time.Second {
							t.Errorf("Unexpected timestamp for user %s: expected around %v, got %v", user.Username, expectedTime, user.CreatedAt)
						}
					}

					previousCreatedAt = user.CreatedAt
				}
			}

			for _, user := range testUsers {
				err := storage.Delete(user.Username)

				if err != nil {
					t.Fatalf("Failed to clean up test user %s: %v", user.Username, err)
				}
			}
		})

		t.Run("List with complex statements", func(t *testing.T) {
			complexUser := &auth.User{
				Username:    "list_test_complex_user",
				Password:    "complex_password",
				Description: "User with complex statements",
				Statements: []auth.AccessKeyStatement{
					{
						Effect:   auth.AccessKeyEffectAllow,
						Resource: "database:app:branch:main:table:users",
						Actions:  []auth.Privilege{auth.DatabasePrivilegeSelect, auth.DatabasePrivilegeInsert},
					},
					{
						Effect:   auth.AccessKeyEffectDeny,
						Resource: "database:app:branch:main:table:users:column:password",
						Actions:  []auth.Privilege{auth.DatabasePrivilegeSelect},
					},
					{
						Effect:   auth.AccessKeyEffectAllow,
						Resource: "database:*:branch:development:*",
						Actions:  []auth.Privilege{"*"},
					},
				},
				CreatedAt: time.Now().UTC(),
				UpdatedAt: time.Now().UTC(),
			}

			err := storage.Store(complexUser)

			if err != nil {
				t.Fatalf("Failed to store complex user: %v", err)
			}

			users, err := storage.List()

			if err != nil {
				t.Fatalf("Failed to list users: %v", err)
			}

			var foundUser *auth.User

			for _, user := range users {
				if user.Username == "list_test_complex_user" {
					foundUser = user
					break
				}
			}

			if foundUser == nil {
				t.Fatal("Expected to find complex test user in list")
			}

			if len(foundUser.Statements) != 3 {
				t.Fatalf("Expected 3 statements, got %d", len(foundUser.Statements))
			}

			stmt := foundUser.Statements[0]
			if stmt.Effect != auth.AccessKeyEffectAllow {
				t.Errorf("Expected first statement effect Allow, got %s", stmt.Effect)
			}

			if stmt.Resource != "database:app:branch:main:table:users" {
				t.Errorf("Expected first statement resource 'database:app:branch:main:table:users', got %s", stmt.Resource)
			}

			if len(stmt.Actions) != 2 {
				t.Errorf("Expected first statement to have 2 actions, got %d", len(stmt.Actions))
			}

			err = storage.Delete("list_test_complex_user")

			if err != nil {
				t.Fatalf("Failed to clean up complex test user: %v", err)
			}
		})

		t.Run("Delete existing user", func(t *testing.T) {
			user := &auth.User{
				Username:    "delete_test_user",
				Password:    "delete_test_password",
				Description: "User to be deleted",
				Statements: []auth.AccessKeyStatement{
					{
						Effect:   auth.AccessKeyEffectAllow,
						Resource: "*",
						Actions:  []auth.Privilege{"*"},
					},
				},
				CreatedAt: time.Now().UTC(),
				UpdatedAt: time.Now().UTC(),
			}

			err := storage.Store(user)

			if err != nil {
				t.Fatalf("Failed to store user for delete test: %v", err)
			}

			_, err = storage.Get("delete_test_user")

			if err != nil {
				t.Fatalf("Failed to get user before deletion: %v", err)
			}

			err = storage.Delete("delete_test_user")

			if err != nil {
				t.Fatalf("Failed to delete user: %v", err)
			}

			_, err = storage.Get("delete_test_user")

			if err == nil {
				t.Error("Expected error when getting deleted user, but got nil")
			}
		})

		t.Run("Delete non-existent user", func(t *testing.T) {
			err := storage.Delete("non_existent_user_for_delete")

			if err == nil {
				t.Error("Expected error when deleting non-existent user, but got nil")
			}

			expectedErrorSubstring := "not found"

			if !strings.Contains(err.Error(), expectedErrorSubstring) {
				t.Errorf("Expected error to contain '%s', got: %v", expectedErrorSubstring, err)
			}
		})

		t.Run("Delete and verify database state", func(t *testing.T) {
			users := []*auth.User{
				{
					Username:    "multi_delete_user_1",
					Password:    "password1",
					Description: "First user",
					Statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
					},
					CreatedAt: time.Now().UTC(),
					UpdatedAt: time.Now().UTC(),
				},
				{
					Username:    "multi_delete_user_2",
					Password:    "password2",
					Description: "Second user",
					Statements: []auth.AccessKeyStatement{
						{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
					},
					CreatedAt: time.Now().UTC(),
					UpdatedAt: time.Now().UTC(),
				},
			}

			for _, user := range users {
				err := storage.Store(user)

				if err != nil {
					t.Fatalf("Failed to store user %s: %v", user.Username, err)
				}
			}

			for _, user := range users {
				_, err := storage.Get(user.Username)

				if err != nil {
					t.Fatalf("Failed to get user %s: %v", user.Username, err)
				}
			}

			err := storage.Delete("multi_delete_user_1")

			if err != nil {
				t.Fatalf("Failed to delete first user: %v", err)
			}

			_, err = storage.Get("multi_delete_user_1")

			if err == nil {
				t.Error("Expected error when getting deleted user, but got nil")
			}

			_, err = storage.Get("multi_delete_user_2")

			if err != nil {
				t.Errorf("Second user should still exist after deleting first user: %v", err)
			}

			err = storage.Delete("multi_delete_user_2")

			if err != nil {
				t.Fatalf("Failed to clean up second user: %v", err)
			}
		})

		t.Run("Delete with special characters in username", func(t *testing.T) {
			specialUsername := "test'user\"with;special--chars"

			user := &auth.User{
				Username:    specialUsername,
				Password:    "special_password",
				Description: "User with special characters",
				Statements: []auth.AccessKeyStatement{
					{
						Effect:   auth.AccessKeyEffectAllow,
						Resource: "*",
						Actions:  []auth.Privilege{"*"},
					},
				},
				CreatedAt: time.Now().UTC(),
				UpdatedAt: time.Now().UTC(),
			}

			err := storage.Store(user)

			if err != nil {
				t.Fatalf("Failed to store user with special characters: %v", err)
			}

			_, err = storage.Get(specialUsername)

			if err != nil {
				t.Fatalf("Failed to get user with special characters: %v", err)
			}

			err = storage.Delete(specialUsername)

			if err != nil {
				t.Fatalf("Failed to delete user with special characters: %v", err)
			}

			_, err = storage.Get(specialUsername)

			if err == nil {
				t.Error("Expected error when getting deleted user with special characters, but got nil")
			}
		})

		t.Run("Delete with empty username", func(t *testing.T) {
			err := storage.Delete("")

			if err == nil {
				t.Error("Expected error when deleting with empty username, but got nil")
			}

			expectedErrorSubstring := "not found"

			if !strings.Contains(err.Error(), expectedErrorSubstring) {
				t.Errorf("Expected error to contain '%s', got: %v", expectedErrorSubstring, err)
			}
		})

		t.Run("Update", func(t *testing.T) {
			t.Run("Update existing user", func(t *testing.T) {
				originalUser := &auth.User{
					Username:    "test-update-user-1",
					Password:    "original-password",
					Description: "Original description",
					Statements: []auth.AccessKeyStatement{
						{
							Effect:   auth.AccessKeyEffectAllow,
							Resource: "database:original",
							Actions:  []auth.Privilege{auth.DatabasePrivilegeRead},
						},
					},
					CreatedAt: time.Now().UTC().Truncate(time.Second),
					UpdatedAt: time.Now().UTC().Truncate(time.Second),
				}

				err := storage.Store(originalUser)

				if err != nil {
					t.Fatalf("Failed to store original user: %v", err)
				}

				updatedUser := &auth.User{
					Username:    "test-update-user-1",
					Password:    "updated-password",
					Description: "Updated description",
					Statements: []auth.AccessKeyStatement{
						{
							Effect:   auth.AccessKeyEffectAllow,
							Resource: "database:updated",
							Actions:  []auth.Privilege{auth.DatabasePrivilegeUpdate},
						},
						{
							Effect:   auth.AccessKeyEffectDeny,
							Resource: "database:restricted",
							Actions:  []auth.Privilege{"*"},
						},
					},
					CreatedAt: originalUser.CreatedAt,           // Should remain the same
					UpdatedAt: time.Now().UTC().Add(time.Hour), // Should be updated
				}

				err = storage.Update(updatedUser)

				if err != nil {
					t.Fatalf("Failed to update user: %v", err)
				}

				retrievedUser, err := storage.Get("test-update-user-1")

				if err != nil {
					t.Fatalf("Failed to get updated user: %v", err)
				}

				if retrievedUser.Password != "updated-password" {
					t.Errorf("Expected Password 'updated-password', got '%s'", retrievedUser.Password)
				}

				if retrievedUser.Description != "Updated description" {
					t.Errorf("Expected Description 'Updated description', got '%s'", retrievedUser.Description)
				}

				if len(retrievedUser.Statements) != 2 {
					t.Fatalf("Expected 2 statements, got %d", len(retrievedUser.Statements))
				}

				if retrievedUser.Statements[0].Resource != "database:updated" {
					t.Errorf("Expected first statement resource 'database:updated', got '%s'", retrievedUser.Statements[0].Resource)
				}

				if retrievedUser.Statements[0].Actions[0] != auth.DatabasePrivilegeUpdate {
					t.Errorf("Expected first statement action 'database:update', got '%s'", retrievedUser.Statements[0].Actions[0])
				}

				if retrievedUser.Statements[1].Effect != auth.AccessKeyEffectDeny {
					t.Errorf("Expected second statement effect 'Deny', got '%s'", retrievedUser.Statements[1].Effect)
				}

				if !retrievedUser.CreatedAt.Equal(originalUser.CreatedAt) {
					t.Errorf("CreatedAt should not change on update. Expected %v, got %v", originalUser.CreatedAt, retrievedUser.CreatedAt)
				}

				if !retrievedUser.UpdatedAt.After(originalUser.UpdatedAt) {
					t.Errorf("UpdatedAt should be newer than original. Original: %v, Updated: %v", originalUser.UpdatedAt, retrievedUser.UpdatedAt)
				}
			})

			t.Run("Update non-existent user", func(t *testing.T) {
				nonExistentUser := &auth.User{
					Username:    "non-existent-user",
					Password:    "password",
					Description: "Description",
					Statements:  []auth.AccessKeyStatement{},
					CreatedAt:   time.Now().UTC(),
					UpdatedAt:   time.Now().UTC(),
				}

				err := storage.Update(nonExistentUser)

				if err == nil {
					t.Error("Expected error when updating non-existent user, but got nil")
				}

				expectedErrorSubstring := "not found"

				if !strings.Contains(err.Error(), expectedErrorSubstring) {
					t.Errorf("Expected error to contain '%s', got: %v", expectedErrorSubstring, err)
				}
			})

			t.Run("Update with empty statements", func(t *testing.T) {
				originalUser := &auth.User{
					Username:    "test-update-empty-statements",
					Password:    "password",
					Description: "Description",
					Statements: []auth.AccessKeyStatement{
						{
							Effect:   auth.AccessKeyEffectAllow,
							Resource: "database:test",
							Actions:  []auth.Privilege{auth.DatabasePrivilegeRead},
						},
					},
					CreatedAt: time.Now().UTC(),
					UpdatedAt: time.Now().UTC(),
				}

				err := storage.Store(originalUser)

				if err != nil {
					t.Fatalf("Failed to store original user: %v", err)
				}

				updatedUser := &auth.User{
					Username:    "test-update-empty-statements",
					Password:    "updated-password",
					Description: "Updated with empty statements",
					Statements:  []auth.AccessKeyStatement{}, // Empty statements
					CreatedAt:   originalUser.CreatedAt,
					UpdatedAt:   time.Now().UTC().Add(time.Hour),
				}

				err = storage.Update(updatedUser)

				if err != nil {
					t.Fatalf("Failed to update user with empty statements: %v", err)
				}

				retrievedUser, err := storage.Get("test-update-empty-statements")

				if err != nil {
					t.Fatalf("Failed to get updated user: %v", err)
				}

				if len(retrievedUser.Statements) != 0 {
					t.Errorf("Expected 0 statements, got %d", len(retrievedUser.Statements))
				}

				if retrievedUser.Description != "Updated with empty statements" {
					t.Errorf("Expected Description 'Updated with empty statements', got '%s'", retrievedUser.Description)
				}
			})

			t.Run("Update with complex statements", func(t *testing.T) {
				originalUser := &auth.User{
					Username:    "test-update-complex",
					Password:    "password",
					Description: "Complex update test",
					Statements: []auth.AccessKeyStatement{
						{
							Effect:   auth.AccessKeyEffectAllow,
							Resource: "database:simple",
							Actions:  []auth.Privilege{auth.DatabasePrivilegeRead},
						},
					},
					CreatedAt: time.Now().UTC(),
					UpdatedAt: time.Now().UTC(),
				}

				err := storage.Store(originalUser)

				if err != nil {
					t.Fatalf("Failed to store original user: %v", err)
				}

				complexStatements := []auth.AccessKeyStatement{
					{
						Effect:   auth.AccessKeyEffectAllow,
						Resource: "database:production/*",
						Actions:  []auth.Privilege{auth.DatabasePrivilegeRead, auth.DatabasePrivilegeSelect},
					},
					{
						Effect:   auth.AccessKeyEffectDeny,
						Resource: "database:sensitive/*",
						Actions:  []auth.Privilege{"*"},
					},
					{
						Effect:   auth.AccessKeyEffectAllow,
						Resource: "database:public/*",
						Actions:  []auth.Privilege{auth.DatabasePrivilegeRead, auth.DatabasePrivilegeUpdate},
					},
				}

				updatedUser := &auth.User{
					Username:    "test-update-complex",
					Password:    "updated-complex-password",
					Description: "Updated with complex statements",
					Statements:  complexStatements,
					CreatedAt:   originalUser.CreatedAt,
					UpdatedAt:   time.Now().UTC().Add(time.Hour),
				}

				err = storage.Update(updatedUser)

				if err != nil {
					t.Fatalf("Failed to update user with complex statements: %v", err)
				}

				retrievedUser, err := storage.Get("test-update-complex")

				if err != nil {
					t.Fatalf("Failed to get updated user: %v", err)
				}

				if len(retrievedUser.Statements) != 3 {
					t.Fatalf("Expected 3 statements, got %d", len(retrievedUser.Statements))
				}

				stmt1 := retrievedUser.Statements[0]

				if stmt1.Effect != auth.AccessKeyEffectAllow {
					t.Errorf("Expected first statement effect 'Allow', got '%s'", stmt1.Effect)
				}

				if stmt1.Resource != "database:production/*" {
					t.Errorf("Expected first statement resource 'database:production/*', got '%s'", stmt1.Resource)
				}

				if len(stmt1.Actions) != 2 {
					t.Fatalf("Expected first statement to have 2 actions, got %d", len(stmt1.Actions))
				}

				stmt2 := retrievedUser.Statements[1]

				if stmt2.Effect != auth.AccessKeyEffectDeny {
					t.Errorf("Expected second statement effect 'Deny', got '%s'", stmt2.Effect)
				}

				if stmt2.Actions[0] != "*" {
					t.Errorf("Expected second statement action '*', got '%s'", stmt2.Actions[0])
				}

				stmt3 := retrievedUser.Statements[2]

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
