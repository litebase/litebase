package auth_test

import (
	"encoding/json"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/server"
	"github.com/litebase/litebase/server/auth"
)

func TestUserManager_Add(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		um := app.Auth.UserManager()

		// Test adding a user
		err := um.Add("testuser", "testpass", []auth.AccessKeyStatement{
			{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []string{"*"}},
		})

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		// Verify user was added
		user := um.Get("testuser")

		if user == nil {
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

		if user.CreatedAt == "" {
			t.Error("Expected CreatedAt to be set")
		}

		if user.UpdatedAt == "" {
			t.Error("Expected UpdatedAt to be set")
		}
	})
}

func TestUserManager_Add_UpdatesExistingUser(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		um := app.Auth.UserManager()

		// Add initial user
		err := um.Add("testuser", "testpass", []auth.AccessKeyStatement{
			{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []string{"*"}},
		})

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		// Update user with new privileges
		err = um.Add("testuser", "newpass", []auth.AccessKeyStatement{
			{Effect: auth.AccessKeyEffectAllow, Resource: "resource1", Actions: []string{"*"}},
			{Effect: auth.AccessKeyEffectAllow, Resource: "resource2", Actions: []string{"*"}},
			{Effect: auth.AccessKeyEffectAllow, Resource: "resource3", Actions: []string{"*"}},
		})

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		// Verify user was updated
		user := um.Get("testuser")

		if user == nil {
			t.Fatal("Expected user to be found")
		}

		if len(user.Statements) != 3 {
			t.Errorf("Expected 3 statements, got %d", len(user.Statements))
		}
	})
}

func TestUserManager_All(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		um := app.Auth.UserManager()

		// Get all users
		users := um.All()

		if len(users) != 1 {
			t.Errorf("Expected 1 users, got %d", len(users))
		}

		// Add multiple users
		err := um.Add("user1", "pass1", []auth.AccessKeyStatement{
			{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []string{"*"}},
		})

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		err = um.Add("user2", "pass2", []auth.AccessKeyStatement{
			{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []string{"write"}},
		})

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		// Get all users
		users = um.All()

		if len(users) != 3 {
			t.Errorf("Expected 3 users, got %d", len(users))
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
}

func TestUserManager_Authenticate(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		um := app.Auth.UserManager()

		// Add a user
		err := um.Add("testuser", "testpass", []auth.AccessKeyStatement{
			{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []string{"*"}},
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
}

func TestUserManager_Get(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		um := app.Auth.UserManager()

		// Add a user
		err := um.Add("testuser", "testpass", []auth.AccessKeyStatement{
			{Effect: auth.AccessKeyEffectAllow, Resource: "resource1", Actions: []string{"*"}},
			{Effect: auth.AccessKeyEffectAllow, Resource: "resource2", Actions: []string{"*"}},
		})

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		// Test getting existing user
		user := um.Get("testuser")
		if user == nil {
			t.Fatal("Expected user to be found")
		}
		if user.Username != "testuser" {
			t.Errorf("Expected username 'testuser', got '%s'", user.Username)
		}

		if len(user.Statements) != 2 {
			t.Errorf("Expected 2 statements, got %d", len(user.Statements))
		}

		// Test getting non-existent user
		user = um.Get("nonexistent")
		if user != nil {
			t.Error("Expected nil for non-existent user")
		}
	})
}

func TestUserManager_Init_WithExistingUsers(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		um := server.App.Auth.UserManager()

		// Add a user first
		err := um.Add("existinguser", "pass", []auth.AccessKeyStatement{
			{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []string{"*"}},
		})

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		server.Shutdown()

		server = test.NewTestServer(t)

		// Create new UserManager instance to test Init
		um2 := server.App.Auth.UserManager()

		// Test Init with existing users
		err = um2.Init()
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		// Verify user was loaded
		user := um2.Get("existinguser")
		if user == nil {
			t.Error("Expected existing user to be loaded")
		}
	})
}

func TestUserManager_Init_WithoutExistingUsers_WithRootCredentials(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		um := app.Auth.UserManager()

		// Test Init without existing users but with root credentials
		err := um.Init()
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		// Verify root user was created
		user := um.Get("root")
		if user == nil {
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

		um := server.App.Auth.UserManager()

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

		um := server.App.Auth.UserManager()

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

func TestUserManager_Remove(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		um := app.Auth.UserManager()

		// Add users
		err := um.Add("user1", "pass1", []auth.AccessKeyStatement{
			{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []string{"read"}},
		})

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}
		err = um.Add("user2", "pass2", []auth.AccessKeyStatement{
			{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []string{"read"}},
		})

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		// Verify both users exist
		if um.Get("user1") == nil {
			t.Error("Expected user1 to exist")
		}
		if um.Get("user2") == nil {
			t.Error("Expected user2 to exist")
		}

		// Remove user1
		err = um.Remove("user1")
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		// Verify user1 was removed and user2 still exists
		if um.Get("user1") != nil {
			t.Error("Expected user1 to be removed")
		}
		if um.Get("user2") == nil {
			t.Error("Expected user2 to still exist")
		}
	})
}

func TestUserManager_Remove_NonExistentUser(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		um := app.Auth.UserManager()

		// Try to remove non-existent user
		err := um.Remove("nonexistent")
		if err != nil {
			t.Fatalf("Expected no error when removing non-existent user, got %v", err)
		}
	})
}

func TestUserManager_WriteFile_Persistence(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		um := app.Auth.UserManager()

		// Add a user
		err := um.Add("testuser", "testpass", []auth.AccessKeyStatement{
			{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []string{"*"}},
		})

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		// Read the file directly to verify persistence
		data, err := app.Cluster.ObjectFS().ReadFile("users.json")

		if err != nil {
			t.Fatalf("Expected no error reading file, got %v", err)
		}

		// Parse the JSON to verify structure
		var users map[string]*auth.User
		err = json.Unmarshal(data, &users)
		if err != nil {
			t.Fatalf("Expected valid JSON, got %v", err)
		}

		// Verify user data in file
		user, exists := users["testuser"]
		if !exists {
			t.Error("Expected testuser to exist in file")
		}
		if user.Username != "testuser" {
			t.Errorf("Expected username 'testuser', got '%s'", user.Username)
		}
		if user.Password == "" {
			t.Error("Expected password hash to be stored")
		}
		if len(user.Statements) != 1 || user.Statements[0].Actions[0] != "*" {
			t.Errorf("Expected statements *, got %v", user.Statements[0].Actions)
		}
	})
}

func TestUser_PasswordHandling(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		um := app.Auth.UserManager()

		// Add a user
		err := um.Add("testuser", "plaintextpass", []auth.AccessKeyStatement{
			{Effect: auth.AccessKeyEffectAllow, Resource: "*", Actions: []string{"read"}},
		})

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		// Get user and verify password is hashed
		user := um.Get("testuser")
		if user == nil {
			t.Fatal("Expected user to be found")
		}
		if user.Password == "plaintextpass" {
			t.Error("Expected password to be hashed, not stored as plaintext")
		}
		if user.Password == "" {
			t.Error("Expected password hash to be stored")
		}

		// Verify we can authenticate with original password
		if !um.Authenticate("testuser", "plaintextpass") {
			t.Error("Expected authentication to succeed with original password")
		}
	})
}
