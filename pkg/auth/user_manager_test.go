package auth_test

import (
	"errors"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/server"
)

func TestUserManager(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		t.Run("Create", func(t *testing.T) {
			um := app.Auth.UserManager

			_, err := um.Create("testuser", "testpass", "", []auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}

			user, err := um.Get("testuser")

			if err != nil {
				t.Fatal("Expected user to be found")
			}

			if user.Username != "testuser" {
				t.Errorf("Expected username 'testuser', got '%s'", user.Username)
			}

			if len(user.Statements) != 1 {
				t.Errorf("Expected 1 statement, got %d", len(user.Statements))
			}

			if user.Statements[0].Effect != "allow" || user.Statements[0].Resource != "*" || user.Statements[0].Actions[0] != "*" {
				t.Errorf("Expected first statement to be allow all, got %v", user.Statements[0])
			}

			if user.CreatedAt.IsZero() {
				t.Error("Expected CreatedAt to be set")
			}

			if user.UpdatedAt.IsZero() {
				t.Error("Expected UpdatedAt to be set")
			}
		})

		t.Run("All", func(t *testing.T) {
			um := app.Auth.UserManager

			// Get all users
			users := um.All()

			if len(users) != 2 {
				t.Errorf("Expected 2 users, got %d", len(users))
			}

			// Add multiple users
			_, err := um.Create("user1", "pass1", "", []auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}

			_, err = um.Create("user2", "pass2", "", []auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"write"}},
			})

			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}

			// Get all users
			users = um.All()

			if len(users) != 4 {
				t.Errorf("Expected 4 users, got %d", len(users))
			}

			// Verify passwords are not included
			for _, user := range users {
				if user.Password != "" {
					t.Error("Expected password to be empty in All() result")
				}
			}

			// Verify usernames are present
			usernames := make(map[string]bool)
			for _, user := range users {
				usernames[user.Username] = true
			}

			if !usernames["user1"] || !usernames["user2"] {
				t.Error("Expected both user1 and user2 to be present")
			}
		})

		t.Run("Authenticate", func(t *testing.T) {
			um := app.Auth.UserManager

			// Add a user
			_, err := um.Create("testuser_Authenticate", "testpass", "", []auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
			})

			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}

			// Test successful authentication
			result := um.Authenticate("testuser", "testpass")

			if !result {
				t.Error("Expected authentication to succeed")
			}

			// Test failed authentication with wrong password
			result = um.Authenticate("testuser", "wrongpass")

			if result {
				t.Error("Expected authentication to fail with wrong password")
			}

			// Test failed authentication with non-existent user
			result = um.Authenticate("nonexistent", "testpass")
			if result {
				t.Error("Expected authentication to fail with non-existent user")
			}
		})

		t.Run("Get", func(t *testing.T) {
			um := app.Auth.UserManager

			// Add a user
			_, err := um.Create("testuser_Get", "testpass", "", []auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "resource1", Actions: []auth.Privilege{"*"}},
				{Effect: auth.StatementEffectAllow, Resource: "resource2", Actions: []auth.Privilege{"*"}},
			})

			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}

			// Test getting existing user
			user, err := um.Get("testuser_Get")

			if err != nil {
				t.Fatal("Expected user to be found")
			}

			if user.Username != "testuser_Get" {
				t.Errorf("Expected username 'testuser_Get', got '%s'", user.Username)
			}

			if len(user.Statements) != 2 {
				t.Errorf("Expected 2 statements, got %d", len(user.Statements))
			}

			// Test getting non-existent user
			_, err = um.Get("nonexistent")

			if err == nil {
				t.Error("Expected error for non-existent user")
			}
		})

		t.Run("Remove", func(t *testing.T) {
			um := app.Auth.UserManager

			// Add users
			_, err := um.Create("user1_Remove", "pass1", "", []auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"read"}},
			})

			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}

			_, err = um.Create("user2_Remove", "pass2", "", []auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"read"}},
			})

			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}

			// Verify both users exist
			if user, _ := um.Get("user1_Remove"); user == nil {
				t.Error("Expected user1_Remove to exist")
			}

			if user, _ := um.Get("user2_Remove"); user == nil {
				t.Error("Expected user2_Remove to exist")
			}

			// Remove user1
			err = um.Remove("user1_Remove")

			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}

			// Verify user1 was removed and user2 still exists
			if user, _ := um.Get("user1_Remove"); user != nil {
				t.Error("Expected user1_Remove to be removed")
			}

			if user, _ := um.Get("user2_Remove"); user == nil {
				t.Error("Expected user2_Remove to still exist")
			}
		})

		t.Run("Remove_NonExistentUser", func(t *testing.T) {
			um := app.Auth.UserManager

			// Try to remove non-existent user
			err := um.Remove("nonexistent")

			if err == nil {
				t.Fatal("Expected error when removing non-existent user")
			}
		})

		t.Run("PasswordHandling", func(t *testing.T) {
			um := app.Auth.UserManager

			// Add a user
			_, err := um.Create("testuser_PasswordHandling", "plaintextpass", "", []auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"read"}},
			})

			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}

			// Get user and verify password is hashed
			user, err := um.Get("testuser_PasswordHandling")

			if err != nil {
				t.Fatal("Expected user to be found")
			}

			if user.Password == "plaintextpass" {
				t.Error("Expected password to be hashed, not stored as plaintext")
			}

			if user.Password == "" {
				t.Error("Expected password hash to be stored")
			}

			// Verify we can authenticate with original password
			if !um.Authenticate("testuser_PasswordHandling", "plaintextpass") {
				t.Error("Expected authentication to succeed with original password")
			}
		})

		t.Run("Update", func(t *testing.T) {
			um := app.Auth.UserManager

			// Add a user
			_, err := um.Create("usertoupdate", "testpass", "", []auth.Statement{
				{Effect: auth.StatementEffectAllow, Resource: "resource1", Actions: []auth.Privilege{"*"}},
				{Effect: auth.StatementEffectAllow, Resource: "resource2", Actions: []auth.Privilege{"*"}},
			})

			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}

			user, err := um.Get("usertoupdate")

			if err != nil {
				t.Fatal("Expected user to be found")
			}

			user.Statements = []auth.Statement{
				{Effect: auth.StatementEffectDeny, Resource: "resource1", Actions: []auth.Privilege{"*"}},
			}

			// Update the user's statements
			err = um.Update(user)

			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}

			// Verify the user's statements were updated
			user, err = um.Get("usertoupdate")

			if err != nil {
				t.Fatal("Expected user to be found")
			}

			if len(user.Statements) != 1 {
				t.Errorf("Expected 1 statement, got %d", len(user.Statements))
				return
			}

			if user.Statements[0].Effect != auth.StatementEffectDeny {
				t.Errorf("Expected effect 'Deny', got '%s'", user.Statements[0].Effect)
			}
		})
	})
}

func TestUserManager_Init_WithExistingUsers(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		um := server.App.Auth.UserManager

		// Add a user first
		_, err := um.Create("existinguser", "pass", "", []auth.Statement{
			{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
		})

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		server.Shutdown()

		server = test.NewTestServer(t)
		defer server.Shutdown()

		// Create new UserManager instance to test Init
		um2 := server.App.Auth.UserManager

		// Test Init with existing users
		err = um2.Init()
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		// Verify user was loaded
		user, err := um2.Get("existinguser")

		if err != nil {
			t.Fatal("Expected existing user to be loaded")
		}

		if user == nil {
			t.Error("Expected existing user to be loaded")
		}
	})
}

func TestUserManager_Init_WithoutExistingUsers_WithRootCredentials(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		um := app.Auth.UserManager

		// Test Init without existing users but with root credentials
		err := um.Init()
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		// Verify root user was created
		user, err := um.Get("root")

		if err != nil {
			t.Fatal("Expected root user to be created")
		}

		if len(user.Statements) != 1 || user.Statements[0].Actions[0] != "*" {
			t.Errorf("Expected root user to have '*' privilege, got %v", user.Statements)
		}
	})
}

func TestUserManager_Init_WithoutExistingUsers_WithoutRootUsername(t *testing.T) {
	test.Run(t, func() {
		t.Setenv("LITEBASE_ROOT_USERNAME", "") // Clear root username for test
		server := test.NewTestServer(t)
		defer server.Shutdown()

		um := server.App.Auth.UserManager

		// Test Init without existing users and without root username
		err := um.Init()

		if err == nil {
			t.Error("Expected error when root username is not set")
		}

		if err.Error() != "the LITEBASE_ROOT_USERNAME environment variable is not set" {
			t.Errorf("Expected specific error message, got %v", err)
		}
	})
}

func TestUserManager_Init_WithoutExistingUsers_WithoutRootPassword(t *testing.T) {
	test.Run(t, func() {
		t.Setenv("LITEBASE_ROOT_USERNAME", "root") // Set root username for test
		t.Setenv("LITEBASE_ROOT_PASSWORD", "")     // Clear root password for test

		server := test.NewTestServer(t)
		defer server.Shutdown()

		um := server.App.Auth.UserManager

		// Test Init without existing users and without root password
		err := um.Init()
		if err == nil {
			t.Error("Expected error when root password is not set")
		}
		if err.Error() != "the LITEBASE_ROOT_PASSWORD environment variable is not set" {
			t.Errorf("Expected specific error message, got %v", err)
		}
	})
}

func TestUserManager_Purge(t *testing.T) {
	test.Run(t, func() {
		server1 := test.NewTestServer(t)
		defer server1.Shutdown()

		server2 := test.NewTestServer(t)
		defer server2.Shutdown()

		_, err := server1.App.Auth.UserManager.Create("testuser_Remove", "testpass", "", []auth.Statement{
			{Effect: auth.StatementEffectAllow, Resource: "*", Actions: []auth.Privilege{"*"}},
		})

		if err != nil {
			t.Fatalf("Expected no error when adding user, got %v", err)
		}

		userFromServer1, err := server1.App.Auth.UserManager.Get("testuser_Remove")

		if err != nil {
			t.Fatal("Expected user to be found on server1")
		}

		if userFromServer1 == nil {
			t.Fatal("Expected user to be found on server1")
		}

		userFromServer2, err := server2.App.Auth.UserManager.Get("testuser_Remove")

		if err != nil {
			t.Fatal("Expected user to be found on server2")
		}

		if userFromServer2 == nil {
			t.Fatal("Expected user to be found on server2")
		}

		remErr := server1.App.Auth.UserManager.Remove("testuser_Remove")

		if remErr != nil {
			t.Fatalf("Expected no error when removing user, got %v", remErr)
		}

		// Verify user is removed from server1
		userFromServer1, err = server1.App.Auth.UserManager.Get("testuser_Remove")

		if err != nil && !errors.Is(err, auth.ErrUserNotFound) {
			t.Fatal("Expected user to be found on server1")
		}

		if userFromServer1 != nil {
			t.Error("Expected user to be nil after removal from server1")
		}

		userFromServer2, err = server2.App.Auth.UserManager.Get("testuser_Remove")

		if err != nil && !errors.Is(err, auth.ErrUserNotFound) {
			t.Fatal("Expected user to be found on server2")
		}

		if userFromServer2 != nil {
			t.Error("Expected user to remain nil on server2")
		}
	})
}
