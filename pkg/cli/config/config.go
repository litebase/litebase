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
	Path           string    `json:"path"`
	Profiles       []Profile `json:"profiles"`

	accessKeyId     string
	accessKeySecret string
	username        string
	password        string
	url             string
}

var ErrMissingClusterURL = errors.New("missing cluster URL")
var ErrorProfileNotFound = errors.New("profile not found, provide a valid profile name or enter cluster auth credentials")

func NewConfiguration(path string) (*Configuration, error) {
	// Replace the $HOME environment variable with the actual path
	configPath := strings.Replace(path, "$HOME", os.Getenv("HOME"), 1)
	var configuration *Configuration
	err := os.MkdirAll(filepath.Dir(configPath), 0750)

	if err != nil {
		return nil, err
	}

	_, err = os.Stat(configPath)

	if os.IsNotExist(err) {
		return &Configuration{
			Path: configPath,
		}, nil
	}

	file, err := os.ReadFile(filepath.Clean(configPath))

	if err != nil {
		panic(err)
	}

	if err := json.Unmarshal(file, &configuration); err != nil {
		return nil, err
	}

	return configuration, nil
}

func (c *Configuration) Save() error {
	jsonData, err := json.MarshalIndent(c, "", "  ")

	if err != nil {
		return err
	}

	err = os.WriteFile(c.Path, jsonData, 0600)

	if err != nil {
		return err
	}

	return nil
}

func (c *Configuration) GetProfiles() []Profile {
	return c.Profiles
}

func (c *Configuration) GetProfile(name string) *Profile {
	for _, profile := range c.Profiles {
		if profile.Name == name {
			return &profile
		}
	}

	return nil
}

func (c *Configuration) AddProfile(profile Profile) error {
	c.Profiles = append(c.Profiles, profile)

	return c.Save()
}

func (c *Configuration) DeleteProfile(name string) error {
	profiles := []Profile{}
	var profileFound bool

	for _, profile := range c.Profiles {
		if profile.Name != name {
			profiles = append(profiles, profile)
			profileFound = true

			break
		}
	}

	if !profileFound {
		return ErrorProfileNotFound
	}

	c.Profiles = profiles

	return c.Save()
}

func (c *Configuration) GetAccessKeyId() string {
	return c.accessKeyId
}

func (c *Configuration) GetAccessKeySecret() string {
	return c.accessKeySecret
}

func (c *Configuration) GetCurrentProfile() (*Profile, error) {
	if c.CurrentProfile == "" {
		profiles := c.GetProfiles()

		if len(profiles) > 0 {
			return &profiles[0], nil
		}

		return nil, ErrorProfileNotFound
	}

	return c.GetProfile(c.CurrentProfile), nil
}

func (c *Configuration) GetPassword() string {
	return c.password
}

func (c *Configuration) GetUrl() string {
	return c.url
}

func (c *Configuration) GetUsername() string {
	return c.username
}

func (c *Configuration) SetAccessKeyId(accessKeyId string) {
	c.accessKeyId = accessKeyId
}

func (c *Configuration) SetAccessKeySecret(accessKeySecret string) {
	c.accessKeySecret = accessKeySecret
}

func (c *Configuration) SetPassword(password string) {
	c.password = password
}

func (c *Configuration) SetUrl(url string) {
	c.url = strings.TrimRight(url, "/")
}

func (c *Configuration) SetUsername(username string) {
	c.username = username
}

func (c *Configuration) SwitchProfile(name string) error {
	profile := c.GetProfile(name)

	if profile == nil {
		return ErrorProfileNotFound
	}

	c.CurrentProfile = name

	return c.Save()
}
