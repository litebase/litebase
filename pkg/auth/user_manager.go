package auth

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/litebase/litebase/pkg/config"

	"golang.org/x/crypto/bcrypt"
)

type UserManager struct {
	auth   *Auth
	config *config.Config
	mutex  *sync.Mutex
	path   string
	users  map[string]*User
}

type User struct {
	Username   string               `json:"username"`
	Password   string               `json:"password,omitempty"`
	Statements []AccessKeyStatement `json:"statements"`
	CreatedAt  string               `json:"created_at"`
	UpdatedAt  string               `json:"updated_at"`
}

// Check if the user has authorization for the given resources and actions
func (u *User) AuthorizeForResource(resources []string, actions []Privilege) bool {
	hasAuthorization := false

	for _, action := range actions {
		for _, resource := range resources {
			if Authorized(u.Statements, resource, action) {
				hasAuthorization = true
				break // No need to check further if one action is authorized
			}
		}
	}

	return hasAuthorization
}

func (auth *Auth) UserManager() *UserManager {
	if auth.userManager == nil {
		auth.userManager = &UserManager{
			auth:   auth,
			config: auth.Config,
			mutex:  &sync.Mutex{},
			path:   "users.json",
			users:  map[string]*User{},
		}
	}

	return auth.userManager
}

// Add a new user
func (u *UserManager) Add(username, password string, statements []AccessKeyStatement) error {
	u.mutex.Lock()
	defer u.mutex.Unlock()

	// Bcrypt the password
	bytes, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)

	if err != nil {
		return err
	}

	if u.users == nil {
		u.users = map[string]*User{}
	}

	u.users[username] = &User{
		Username:   username,
		Password:   string(bytes),
		Statements: statements,
		CreatedAt:  time.Now().UTC().Format("2006-01-02 15:04:05"),
		UpdatedAt:  time.Now().UTC().Format("2006-01-02 15:04:05"),
	}

	return u.writeFile()
}

// Return all users without passwords
func (u *UserManager) All() []User {
	u.mutex.Lock()
	defer u.mutex.Unlock()

	// Remove the password from the users without affecting the original
	users := []User{}

	for _, user := range u.users {
		users = append(users, User{
			Username:   user.Username,
			Statements: user.Statements,
			CreatedAt:  user.CreatedAt,
			UpdatedAt:  user.UpdatedAt,
		})
	}

	return users
}

// Read all the users from storage
func (u *UserManager) allUsers() (map[string]*User, error) {
	var users map[string]*User
	file, err := u.auth.ObjectFS.ReadFile(u.path)

	if err != nil && os.IsNotExist(err) {
		_, err = u.auth.ObjectFS.Create(u.path)

		if err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}

	if len(file) == 0 {
		return users, nil
	}

	err = json.Unmarshal(file, &users)

	if err != nil {
		return nil, err
	}

	return users, err
}

// Authenticate a user with username and password
func (u *UserManager) Authenticate(username, password string) bool {
	u.mutex.Lock()
	defer u.mutex.Unlock()

	for _, user := range u.users {
		if user.Username == username {
			err := bcrypt.CompareHashAndPassword([]byte(user.Password), []byte(password))

			if err != nil {
				return false
			}

			return true // Password matches
		}
	}

	return false
}

// Get a user by username
func (u *UserManager) Get(username string) *User {
	u.mutex.Lock()
	defer u.mutex.Unlock()

	for _, user := range u.users {
		if user.Username == username {
			return user
		}
	}

	u.users, _ = u.allUsers()

	for _, user := range u.users {
		if user.Username == username {
			return user
		}
	}

	return nil
}

// Initialize the UserManager
func (u *UserManager) Init() error {
	// Get the users
	users, err := u.allUsers()

	if err != nil {
		return err
	}

	u.mutex.Lock()
	u.users = users
	u.mutex.Unlock()

	if len(users) == 0 {
		if u.config.RootUsername == "" {
			return fmt.Errorf("the LITEBASE_ROOT_USERNAME environment variable is not set")
		}

		if u.config.RootPassword == "" {
			return fmt.Errorf("the LITEBASE_ROOT_PASSWORD environment variable is not set")
		}

		err := u.Add(u.config.RootUsername, u.config.RootPassword, []AccessKeyStatement{
			{
				Effect:   "Allow",
				Resource: "*",
				Actions:  []Privilege{"*"},
			},
		})

		if err != nil {
			return err
		}
	}

	return nil
}

// Purge a user by username from memory
func (u *UserManager) Purge(username string) error {
	u.mutex.Lock()
	defer u.mutex.Unlock()

	// Remove the user from the map
	delete(u.users, username)

	u.auth.Broadcast("user:purge", username)

	return nil
}

// Remove a user by username
func (u *UserManager) Remove(username string) error {
	u.mutex.Lock()
	defer u.mutex.Unlock()

	delete(u.users, username)

	u.auth.Broadcast("user:purge", username)

	return u.writeFile()
}

// Write the users to storage
func (u *UserManager) writeFile() error {
	data, err := json.MarshalIndent(u.users, "", "  ")

	if err != nil {
		return err
	}

	err = u.auth.ObjectFS.WriteFile(u.path, data, 0644)

	return err
}
