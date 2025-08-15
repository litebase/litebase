package auth

import (
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/litebase/litebase/pkg/config"

	"golang.org/x/crypto/bcrypt"
)

type UserManager struct {
	auth        *Auth
	config      *config.Config
	mutex       *sync.Mutex
	users       map[string]*User
	userStorage UserStorage
}

type UserStorage interface {
	Delete(username string) error
	Get(username string) (*User, error)
	List() ([]*User, error)
	Store(user *User) error
	Update(user *User) error
}

// Get the UserManager instance
func NewUserManager(
	userStorage UserStorage,
	auth *Auth,
	config *config.Config,
) *UserManager {
	return &UserManager{
		auth:        auth,
		config:      config,
		mutex:       &sync.Mutex{},
		userStorage: userStorage,
		users:       map[string]*User{},
	}
}

func (u *UserManager) Create(username, password string, statements []AccessKeyStatement) (*User, error) {
	u.mutex.Lock()
	defer u.mutex.Unlock()

	// Bcrypt the password
	bytes, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)

	if err != nil {
		return nil, err
	}

	user := &User{
		Username:   username,
		Password:   string(bytes),
		Statements: statements,
		UpdatedAt:  time.Now().UTC(),
	}

	user.CreatedAt = time.Now().UTC()
	err = u.userStorage.Store(user)

	if err != nil {
		return nil, err
	}

	if u.users == nil {
		u.users = map[string]*User{}
	}

	u.users[username] = user

	return user, nil
}

// Return all users without passwords
func (u *UserManager) All() []User {
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
	users, err := u.userStorage.List()
	if err != nil {
		return nil, err
	}

	userMap := make(map[string]*User)
	for _, user := range users {
		userMap[user.Username] = user
	}

	return userMap, nil
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

	user, err := u.userStorage.Get(username)

	if err != nil {
		slog.Debug("Error getting user from storage", "error", err)
		return nil
	}

	u.users[username] = user

	return user
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

		_, err := u.Create(u.config.RootUsername, u.config.RootPassword, []AccessKeyStatement{
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

	return nil
}

// Remove a user by username
func (u *UserManager) Remove(username string) error {
	u.mutex.Lock()
	defer u.mutex.Unlock()

	delete(u.users, username)

	err := u.userStorage.Delete(username)
	if err != nil {
		return err
	}

	// Broadcast purge event to other servers
	if u.auth != nil {
		u.auth.Broadcast("user:purge", username)
	}

	return nil
}

// Update an existing user
func (u *UserManager) Update(user *User) error {
	u.mutex.Lock()
	defer u.mutex.Unlock()

	existingUser, exists := u.users[user.Username]

	if !exists {
		return fmt.Errorf("the user was not found")
	}

	existingUser.Statements = user.Statements
	existingUser.UpdatedAt = time.Now().UTC()

	return u.userStorage.Update(existingUser)
}
