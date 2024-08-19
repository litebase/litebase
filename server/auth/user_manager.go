package auth

import (
	"encoding/json"
	"fmt"
	"litebase/internal/config"
	"litebase/server/storage"
	"os"
	"sync"
	"time"

	"golang.org/x/crypto/bcrypt"
)

type UserManagerInstance struct {
	mutex *sync.Mutex
	path  string
	users map[string]*User
}

type User struct {
	Username   string   `json:"username"`
	Password   string   `json:"password,omitempty"`
	Privileges []string `json:"privileges"`
	CreatedAt  string   `json:"created_at"`
	UpdatedAt  string   `json:"updated_at"`
}

var staticUserManager *UserManagerInstance

func UserManager() *UserManagerInstance {
	if staticUserManager == nil {
		staticUserManager = &UserManagerInstance{
			mutex: &sync.Mutex{},
			path:  fmt.Sprintf("%s/%s", config.Get().DataPath, "users.json"),
			users: map[string]*User{},
		}
	}

	return staticUserManager
}

func (u *UserManagerInstance) Init() error {
	// Get the users
	users, err := u.allUsers()

	if err != nil {
		return err
	}

	u.mutex.Lock()
	u.users = users
	u.mutex.Unlock()

	if len(users) == 0 {
		if config.Get().RootUsername == "" {
			return fmt.Errorf("the LITEBASE_ROOT_USERNAME environment variable is not set")
		}

		if config.Get().RootPassword == "" {
			return fmt.Errorf("the LITEBASE_ROOT_PASSWORD environment variable is not set")
		}

		err := u.Add(config.Get().RootUsername, config.Get().RootPassword, []string{"*"})

		if err != nil {
			return err
		}
	}

	return nil
}

func (u *UserManagerInstance) All() []User {
	u.mutex.Lock()
	defer u.mutex.Unlock()

	// Remove the password from the users without affecting the original
	users := []User{}

	for _, user := range u.users {
		users = append(users, User{
			Username:   user.Username,
			Privileges: user.Privileges,
			CreatedAt:  user.CreatedAt,
			UpdatedAt:  user.UpdatedAt,
		})
	}

	return users
}

func (u *UserManagerInstance) allUsers() (map[string]*User, error) {
	var users map[string]*User
	file, err := storage.FS().ReadFile(u.path)

	if err != nil && os.IsNotExist(err) {
		_, err = storage.FS().Create(u.path)

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

func (u *UserManagerInstance) Authenticate(username, password string) bool {
	u.mutex.Lock()
	defer u.mutex.Unlock()

	for _, user := range u.users {
		if user.Username == username {
			err := bcrypt.CompareHashAndPassword([]byte(user.Password), []byte(password))

			if err != nil {
				return false
			}
		}
	}

	return true
}

func (u *UserManagerInstance) Get(username string) *User {
	u.mutex.Lock()
	defer u.mutex.Unlock()

	for _, user := range u.users {
		if user.Username == username {
			return user
		}
	}

	return nil
}

func (u *UserManagerInstance) Remove(username string) error {
	u.mutex.Lock()
	defer u.mutex.Unlock()

	delete(u.users, username)

	return u.writeFile()
}

func (u *UserManagerInstance) Add(username, password string, privleges []string) error {
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
		Privileges: privleges,
		CreatedAt:  time.Now().Format("2006-01-02 15:04:05"),
		UpdatedAt:  time.Now().Format("2006-01-02 15:04:05"),
	}

	return u.writeFile()
}

func (u *UserManagerInstance) writeFile() error {
	data, err := json.MarshalIndent(u.users, "", "  ")

	if err != nil {
		return err
	}

	err = storage.FS().WriteFile(u.path, data, 0644)

	return err
}
