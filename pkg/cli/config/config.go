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

	accessKeyId     string
	accessKeySecret string
	interactive     bool
	password        string
	path            string
	url             string
	username        string
}

var ErrMissingClusterURL = errors.New("missing cluster URL")
var ErrorCredentialsNotSet = errors.New("credentials were not set, please provide access credentials or a stored profile name")
var ErrorProfileNotFound = errors.New("profile not found, provide a valid profile name")

// Create a new configuration instance.
func NewConfiguration(path string) (*Configuration, error) {
	var configPath string
	var configuration *Configuration

	if path == "" {
		homeDir, err := os.UserHomeDir()

		if err != nil {
			return nil, err
		}

		configPath = filepath.Join(homeDir, ".litebase", "config.json")
	} else {
		configPath = path
	}

	err := os.MkdirAll(filepath.Dir(configPath), 0750)

	if err != nil {
		return nil, err
	}

	_, err = os.Stat(configPath)

	if os.IsNotExist(err) {
		c := &Configuration{
			path:        configPath,
			interactive: true,
		}

		return c, c.Save()
	}

	file, err := os.ReadFile(filepath.Clean(configPath))

	if err != nil {
		panic(err)
	}

	if err := json.Unmarshal(file, &configuration); err != nil {
		return nil, err
	}

	configuration.path = configPath
	configuration.interactive = true

	return configuration, nil
}

// Add a new profile to the configuration.
func (c *Configuration) AddProfile(profile Profile) error {
	c.Profiles = append(c.Profiles, profile)

	return c.Save()
}

func (c *Configuration) GetInteractive() bool {
	return c.interactive
}

// Get the profiles of the configuration.
func (c *Configuration) GetProfiles() []Profile {
	return c.Profiles
}

// Get a specific profile by name.
func (c *Configuration) GetProfile(name string) *Profile {
	for _, profile := range c.Profiles {
		if profile.Name == name {
			return &profile
		}
	}

	return nil
}

// Delete a profile from the configuration by name.
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

// Return the access key ID used for authentication.
func (c *Configuration) GetAccessKeyId() string {
	return c.accessKeyId
}

// Return the access key secret used for authentication.
func (c *Configuration) GetAccessKeySecret() string {
	return c.accessKeySecret
}

// Return the current profile or the first profile if no current profile is set.
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

// Return the password used for authentication.
func (c *Configuration) GetPassword() string {
	return c.password
}

// Return the URL of the cluster.
func (c *Configuration) GetUrl() string {
	return c.url
}

// Return the username used for authentication.
func (c *Configuration) GetUsername() string {
	return c.username
}

// Save the configuration to the file system.
func (c *Configuration) Save() error {
	jsonData, err := json.MarshalIndent(c, "", "  ")

	if err != nil {
		return err
	}

	err = os.WriteFile(c.path, jsonData, 0600)

	if err != nil {
		return err
	}

	return nil
}

func (c *Configuration) SetAccessKeyId(accessKeyId string) {
	c.accessKeyId = accessKeyId
}

func (c *Configuration) SetAccessKeySecret(accessKeySecret string) {
	c.accessKeySecret = accessKeySecret
}

func (c *Configuration) SetInteractive(interactive bool) {
	c.interactive = interactive
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
