package auth

import (
	"encoding/json"
	"fmt"
	"litebasedb/internal/config"
	"os"
	"sync"

	"golang.org/x/crypto/bcrypt"
)

type UserManagerInstance struct {
	mutex *sync.Mutex
	path  string
	users map[string]string
}

var staticUserManager *UserManagerInstance

func UserManager() *UserManagerInstance {
	if staticUserManager == nil {
		staticUserManager = &UserManagerInstance{
			mutex: &sync.Mutex{},
			path:  fmt.Sprintf("%s/%s/%s", config.Get("data_path"), ".litebasedb", "users.json"),
			users: make(map[string]string),
		}
	}

	return staticUserManager
}

func (u *UserManagerInstance) Init() {
	// Get the users
	users, err := u.allUsers()

	if err != nil {
		panic(err)
	}

	u.mutex.Lock()
	u.users = users
	u.mutex.Unlock()

	if len(users) == 0 {
		err := u.Set("root", config.Get("root_password"))

		if err != nil {
			panic(err)
		}
	}
}

func (u *UserManagerInstance) allUsers() (map[string]string, error) {
	var users = map[string]string{}

	file, err := os.ReadFile(u.path)

	if err != nil && os.IsNotExist(err) {
		_, err = os.Create(u.path)

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

	return users, err
}

func (u *UserManagerInstance) Authenticate(username, password string) bool {
	return false
}

func (u *UserManagerInstance) Remove(username string) error {
	u.mutex.Lock()
	delete(u.users, username)
	u.mutex.Unlock()

	return u.writeFile()
}

func (u *UserManagerInstance) Set(username, password string) error {
	u.mutex.Lock()
	defer u.mutex.Unlock()
	// Bcrypt the password

	bytes, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)

	if err != nil {
		return err
	}

	u.users[username] = string(bytes)

	return u.writeFile()
}

func (u *UserManagerInstance) writeFile() error {
	file, err := os.OpenFile(u.path, os.O_WRONLY|os.O_CREATE, 0644)

	if err != nil {
		return err
	}

	defer file.Close()

	data, err := json.Marshal(u.users)

	if err != nil {
		return err
	}

	_, err = file.Write(data)

	return err
}
