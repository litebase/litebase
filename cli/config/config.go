package config

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
)

type Configuration struct {
	CurrentProfile string    `json:"currentProfile"`
	Profiles       []Profile `json:"profiles"`

	username string
	password string
	url      string
}

var configPath string
var configuration *Configuration

var ErrorProfileNotFound = errors.New("Profile not found")

func Init(path string) error {
	// Replace the $HOME environment variable with the actual path
	path = strings.Replace(path, "$HOME", os.Getenv("HOME"), 1)
	configPath = path

	os.MkdirAll(filepath.Dir(configPath), 0755)

	_, err := os.Stat(configPath)

	if os.IsNotExist(err) {
		configuration = &Configuration{}
		return Save()
	}

	file, err := os.ReadFile(configPath)

	if err != nil {
		panic(err)
	}

	return json.Unmarshal(file, &configuration)
}

func Save() error {
	jsonData, err := json.MarshalIndent(configuration, "", "  ")

	if err != nil {
		return err
	}

	err = os.WriteFile(configPath, jsonData, 0644)

	return err
}

func GetProfiles() []Profile {
	return configuration.Profiles
}

func GetProfile(name string) *Profile {
	for _, profile := range configuration.Profiles {
		if profile.Name == name {
			return &profile
		}
	}

	return nil
}

func AddProfile(profile Profile) error {
	configuration.Profiles = append(configuration.Profiles, profile)

	return Save()
}

func DeleteProfile(name string) error {
	profiles := []Profile{}
	var profileFound bool

	for _, profile := range configuration.Profiles {
		if profile.Name != name {
			profiles = append(profiles, profile)
			profileFound = true

			break
		}
	}

	if !profileFound {
		return ErrorProfileNotFound
	}

	configuration.Profiles = profiles

	return Save()
}

func GetCurrentProfile() *Profile {
	if configuration.CurrentProfile == "" {
		return &GetProfiles()[0]
	}

	return GetProfile(configuration.CurrentProfile)
}

func SwitchProfile(name string) error {
	profile := GetProfile(name)

	if profile == nil {
		return ErrorProfileNotFound
	}

	configuration.CurrentProfile = name

	return Save()
}

func SetUrl(url string) {
	configuration.url = url
}

func SetPassword(password string) {
	configuration.password = password
}

func SetUsername(username string) {
	configuration.username = username
}

func GetUrl() string {
	return configuration.url
}

func GetPassword() string {
	return configuration.password
}

func GetUsername() string {
	return configuration.username
}
